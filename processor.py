import pandas as pd
import numpy as np
from datetime import datetime
import os
import io

class ExcelProcessor:
    def __init__(self):
        self.raw_data = [] # List of dataframes
        self.all_columns = set()
        self.priority_columns = ["Timestamp", "Coin", "Quantity", "$", "INR"]
        self.synonyms = {
            "$": ["$", "dollar", "usd", "amount"],
            "timestamp": ["timestamp", "datetime", "date time"],
            "date": ["date"],
            "time": ["time"],
            "coin": ["coin", "crypto"],
            "quantity": ["quantity", "qty"],
            "inr": ["inr", "rupees"]
        }
    
    def load_files(self, source):
        """Read all sheets from uploaded files or a folder path."""
        self.raw_data = []
        
        # Determine source type
        is_path = isinstance(source, str) and os.path.isdir(source)
        files_to_load = []
        
        if is_path:
            # List all .xlsx files in folder
            for f in os.listdir(source):
                if f.endswith(".xlsx") and not f.startswith("~$"): # Ignore temporary/locked files
                    files_to_load.append(os.path.join(source, f))
        else:
            # Source is list of uploaded files (Streamlit objects)
            files_to_load = source

        for file_item in files_to_load:
            try:
                # Handle both path strings and file objects
                name = os.path.basename(file_item) if isinstance(file_item, str) else file_item.name
                xls = pd.ExcelFile(file_item)
                
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    df = df.dropna(how='all')
                    
                    if df.empty:
                        continue
                        
                    # Add metadata as required: WORKBOOK NAME and SHEET NAME
                    df["WORKBOOK NAME"] = name
                    df["SHEET NAME"] = sheet_name
                    
                    self.raw_data.append(df)
            except Exception as e:
                print(f"Error loading {name}: {e}")
        return self.raw_data

    def normalize_columns(self, df):
        """Trim spaces, lowercase (except metadata), and remove duplicate columns."""
        # Normalize column names - preserve metadata casing
        protected = ["WORKBOOK NAME", "SHEET NAME"]
        new_cols = []
        for c in df.columns:
            c_str = str(c).strip()
            if c_str in protected:
                new_cols.append(c_str)
            else:
                new_cols.append(c_str.lower())
        df.columns = new_cols
        
        # Remove duplicate column names within sheet (keep first)
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    def map_columns(self, df):
        """Apply synonym mapping and conflict handling."""
        # Standard names for priority columns
        priority_map = {
            "$": "$",
            "timestamp": "Timestamp",
            "coin": "Coin",
            "quantity": "Quantity",
            "inr": "INR",
            "date": "date",
            "time": "time"
        }
        
        processed_df = df.copy()
        
        # Mapping groups based on required synonyms
        mapping_groups = {
            "$": ["$", "dollar", "usd", "amount"],
            "timestamp": ["timestamp", "datetime", "date time"],
            "date": ["date"],
            "time": ["time"],
            "coin": ["coin", "crypto"],
            "quantity": ["quantity", "qty"],
            "inr": ["inr", "rupees"]
        }

        for key, syns in mapping_groups.items():
            std_name = priority_map[key]
            matches = [c for c in processed_df.columns if c in [s.lower() for s in syns]]
            if not matches:
                continue
            
            # If standard name (normalized to lower for check) is among matches, keep it
            if std_name.lower() in matches:
                # Keep the one that is exactly std_name.lower() or rename it to std_name
                target = std_name.lower()
                processed_df = processed_df.rename(columns={target: std_name})
                # Drop all other synonyms
                for m in matches:
                    if m != target:
                        processed_df = processed_df.drop(columns=[m])
            else:
                # Keep first synonym and rename to standard
                keep = matches[0]
                processed_df = processed_df.rename(columns={keep: std_name})
                for m in matches[1:]:
                    processed_df = processed_df.drop(columns=[m])
        
        # Remove duplicate columns again
        processed_df = processed_df.loc[:, ~processed_df.columns.duplicated()]
        return processed_df

    def parse_timestamp(self, series):
        """Robust timestamp parsing with dayfirst support and Excel serial handling."""
        def _parse(val):
            if pd.isna(val) or val == "":
                return pd.NaT
            
            # Handle float/int (Excel serial dates)
            if isinstance(val, (int, float)):
                try:
                    return pd.to_datetime(val, unit='D', origin='1899-12-30').round('S')
                except:
                    return pd.NaT
            
            s_val = str(val).strip()
            if not s_val:
                return pd.NaT
            
            # Try dayfirst=True
            try:
                dt = pd.to_datetime(s_val, dayfirst=True, errors='coerce')
                if not pd.isna(dt):
                    return dt.round('s')
            except:
                pass
            
            # Try dayfirst=False
            try:
                dt = pd.to_datetime(s_val, dayfirst=False, errors='coerce')
                if not pd.isna(dt):
                    return dt.round('s')
            except:
                pass
                
            return pd.NaT

        return series.apply(_parse)

    def handle_timestamp_logic(self, df):
        """Merge DATE+TIME, handle single columns, etc."""
        cols = df.columns
        # Note: map_columns renames internal helpers to 'date' and 'time'
        # and main timestamp to 'Timestamp'
        has_timestamp = "Timestamp" in cols
        has_date = "date" in cols
        has_time = "time" in cols
        
        # Case 1: DATE + TIME
        if has_date and has_time:
            dates = self.parse_timestamp(df["date"])
            times = pd.to_datetime(df["time"], errors='coerce')
            
            def merge_dt(d, t):
                if pd.isna(d): return pd.NaT
                if pd.isna(t):
                    return d.replace(hour=0, minute=0, second=0)
                try:
                    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)
                except:
                    return d.replace(hour=0, minute=0, second=0)
            
            df["Timestamp"] = [merge_dt(d, t) for d, t in zip(dates, times)]
            
        elif has_date:
            # Case 2: Only DATE exists
            dates = self.parse_timestamp(df["date"])
            df["Timestamp"] = [d.replace(hour=0, minute=0, second=0) if not pd.isna(d) else pd.NaT for d in dates]
            
        elif has_time:
            # Case 3: TIME exists but no DATE
            df["Timestamp"] = pd.NaT
            # Tag the dataframe so validate_rows knows this was a time-only case
            df["_time_only_flag"] = True
            
        elif has_timestamp:
            # Case 4: Existing Timestamp column normalize
            df["Timestamp"] = self.parse_timestamp(df["Timestamp"])
        else:
            df["Timestamp"] = pd.NaT
            
        return df

    def validate_rows(self, df):
        """Identify invalid rows and add error reasons."""
        df["ERROR REASON"] = ""
        cols = df.columns
        
        # Reasons
        # Use the flag we set in handle_timestamp_logic
        has_time_no_date = "_time_only_flag" in df.columns
        
        # Mark rows
        if has_time_no_date:
            mask_time = df["time"].notna()
            df.loc[mask_time, "ERROR REASON"] = "TIME without DATE"
            # Cleanup flag
            df = df.drop(columns=["_time_only_flag"])
            
        # Invalid format mask
        mask_invalid = df["Timestamp"].isna()
        # Only set if reason is not already set
        df.loc[mask_invalid & (df["ERROR REASON"] == ""), "ERROR REASON"] = "Invalid timestamp format"
        
        # If absolutely no timestamp related columns
        if not any(c in cols for c in ["Timestamp", "date", "time"]):
            df["ERROR REASON"] = "Missing timestamp column"

        return df

    def process(self, source, user_cols, filter_values, ts_hint):
        """Main processing pipeline with Dynamic Data Selection (filtering)."""
        all_processed = []
        
        # 1. Load files (from folder or uploads)
        dfs = self.load_files(source)
        if not dfs:
            return None, None, None, "No data found in source."

        # 2. Pre-process each sheet
        for df in dfs:
            df = self.normalize_columns(df)
            df = self.map_columns(df)
            df = self.handle_timestamp_logic(df)
            df = self.validate_rows(df)
            all_processed.append(df)
            
        # 3. Merge data
        combined_df = pd.concat(all_processed, axis=0, ignore_index=True, sort=False)
        
        # 4. Column Matchup (for keys and filters)
        norm_user_cols = [c.strip().lower() for c in user_cols]
        # Map user input to standardized names if they match
        def find_col(user_input, df_cols):
            p_map = {"timestamp": "Timestamp", "coin": "Coin", "quantity": "Quantity", "$": "$", "inr": "INR", "date": "date", "time": "time"}
            key = user_input.lower()
            if key in p_map:
                std = p_map[key]
                if std in df_cols: return std
            
            s_map = {
                "$": ["$", "dollar", "usd", "amount"],
                "timestamp": ["timestamp", "datetime", "date time"],
                "date": ["date"],
                "time": ["time"],
                "coin": ["coin", "crypto"],
                "quantity": ["quantity", "qty"],
                "inr": ["inr", "rupees"]
            }
            for k, syns in s_map.items():
                if key in [s.lower() for s in syns]:
                    std = p_map[k]
                    if std in df_cols: return std
            
            for c in df_cols:
                if str(c).lower() == key: return c
            return None

        matched_cols = []
        for uc in norm_user_cols:
            found = find_col(uc, combined_df.columns)
            if found:
                matched_cols.append(found)
            else:
                return None, None, None, f"Configuration Error: Column '{uc}' not found in the dataset."

        if "Timestamp" not in matched_cols:
            return None, None, None, "Validation Error: One of the 3 columns must be a Timestamp column."

        # 5. Dynamic Data Selection (Filtering)
        # We apply filtering to the combined_df before splitting valid/invalid? 
        # Usually, filtering happens on valid data, but user said "select the data from files".
        # We'll filter the whole combined_df.
        
        # Mapping filter values to matched columns
        # filter_values is a list of 3 strings corresponding to user_cols
        for col_name, f_val in zip(matched_cols, filter_values):
            if not f_val or str(f_val).strip() == "":
                continue
            
            f_val = str(f_val).strip().lower()
            
            if col_name == "Timestamp":
                # Flexible matching: if user gives '05:10', match 05:10:00 to 05:10:59
                # We format to the most detailed string and check for containment
                full_fmt = "%d:%m:%Y %H:%M:%S"
                
                # Check if the user's filter value is contained within the formatted timestamp string
                # This allows filtering by Date, Time, or both dynamically.
                mask = combined_df["Timestamp"].apply(
                    lambda x: x.strftime(full_fmt) if not pd.isna(x) else ""
                ).str.lower().str.contains(f_val, regex=False)
                
                combined_df = combined_df[mask]
            else:
                # Regular column filter
                matches_col = combined_df[col_name].astype(str).str.strip().str.lower() == f_val
                combined_df = combined_df[matches_col]

        # 6. Split into VALID and INVALID
        invalid_mask = combined_df["ERROR REASON"] != ""
        valid_df = combined_df[~invalid_mask].copy()
        invalid_df = combined_df[invalid_mask].copy()

        # Final Formatting for Valid Timestamp
        if not valid_df.empty:
            valid_df["Timestamp"] = valid_df["Timestamp"].dt.strftime("%d:%m:%Y %H:%M:%S")

        # 7. Duplicate Detection
        if not valid_df.empty:
            def get_key(row):
                key_parts = []
                all_missing = True
                for c in matched_cols:
                    val = str(row[c]).strip().lower() if not pd.isna(row[c]) else "missing_placeholder"
                    if val != "missing_placeholder": all_missing = False
                    key_parts.append(val)
                return None if all_missing else "|".join(key_parts)

            valid_df["_key"] = valid_df.apply(get_key, axis=1)
            valid_df["DUPLICATE"] = "Unique"
            key_mask = valid_df["_key"].notna()
            valid_df.loc[key_mask, "DUPLICATE"] = np.where(valid_df[key_mask].duplicated(subset=["_key"], keep='first'), "Duplicate", "Unique")
            
            # --- Aggregation for Distinct Data ---
            # We want one row per _key, but showing ALL workbooks and sheets
            # For records where _key is None (all null), they are already considered Unique and not aggregated.
            
            non_null_keys = valid_df[valid_df["_key"].notna()]
            null_keys = valid_df[valid_df["_key"].isna()]
            
            if not non_null_keys.empty:
                # Group and aggregate
                agg_funcs = {c: "first" for c in non_null_keys.columns if c not in ["WORKBOOK NAME", "SHEET NAME", "_key", "DUPLICATE"]}
                agg_funcs["WORKBOOK NAME"] = lambda x: ", ".join(sorted(set(str(v) for v in x if not pd.isna(v))))
                agg_funcs["SHEET NAME"] = lambda x: ", ".join(sorted(set(str(v) for v in x if not pd.isna(v))))
                
                distinct_non_null = non_null_keys.groupby("_key", as_index=False).agg(agg_funcs)
            else:
                distinct_non_null = pd.DataFrame(columns=valid_df.columns)

            distinct_df = pd.concat([distinct_non_null, null_keys], ignore_index=True)
            distinct_df = distinct_df.drop(columns=["_key", "DUPLICATE"])
            
            valid_df = valid_df.drop(columns=["_key"])
        else:
            distinct_df = valid_df.copy()

        # 8. Final Ordering (Strictly preserves WORKBOOK NAME and SHEET NAME)
        def apply_final_order(df, is_invalid=False):
            if df.empty: return df
            for c in self.priority_columns:
                if c not in df.columns: df[c] = ""
            meta = ["DUPLICATE", "WORKBOOK NAME", "SHEET NAME"]
            for c in meta:
                if c not in df.columns: df[c] = ""
            
            cols = df.columns.tolist()
            priority = [c for c in self.priority_columns]
            metadata = [c for c in meta]
            others = [c for c in cols if c not in priority and c not in metadata and c != "ERROR REASON"]
            
            final = priority + others
            if "ERROR REASON" in cols: final += ["ERROR REASON"]
            final += metadata
            return df[final]

        all_data = apply_final_order(valid_df)
        distinct_data = apply_final_order(distinct_df)
        invalid_data = apply_final_order(invalid_df, is_invalid=True)

        return all_data, distinct_data, invalid_data, None
