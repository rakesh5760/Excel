import pandas as pd
import numpy as np
from datetime import datetime
import os
import io

class ExcelProcessor:
    def __init__(self):
        self.raw_data = [] # List of dataframes
        self.load_errors = [] # To capture file-level failures
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
                err_msg = f"Error loading {name}: {str(e)}"
                print(err_msg)
                self.load_errors.append(err_msg)
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
            
            # Try standard parsing first
            try:
                dt = pd.to_datetime(s_val, dayfirst=True, errors='coerce')
                if not pd.isna(dt): return dt.round('s')
            except: pass

            # Handle messy strings (e.g. "[1:17 pm, 23/04/2026] Text")
            import re
            
            # Pattern 1: Numeric dates (23/04/2026 or 2026-04-23)
            # Pattern 2: Text dates (7 September 2020 or Nov-10-2021)
            patterns = [
                r'(\d{1,4}[-/.]\d{1,4}[-/.]\d{1,4})',
                r'(\d{1,2}\s+[a-z]+\s+\d{4})',
                r'([a-z]+\s+\d{1,2},?\s+\d{4})',
                r'([a-z]{3}-\d{1,2}-\d{4})'
            ]
            
            extracted = None
            for p in patterns:
                match = re.search(p, s_val, re.IGNORECASE)
                if match:
                    extracted = match.group(1)
                    break
            
            if extracted:
                # Add time if found
                time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?(\s*[ap]m)?)', s_val, re.IGNORECASE)
                if time_match:
                    extracted += " " + time_match.group(1)
                
                try:
                    dt = pd.to_datetime(extracted, dayfirst=True, errors='coerce')
                    if not pd.isna(dt): return dt.round('s')
                except: pass
                
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
        df.loc[mask_invalid & (df["ERROR REASON"] == ""), "ERROR REASON"] = "Invalid timestamp format"

        # 2016-2026 Range Check
        # Valid from 01-01-2016 to 31-12-2026
        start_date = datetime(2016, 1, 1)
        end_date = datetime(2026, 12, 31, 23, 59, 59)
        
        valid_ts_mask = df["Timestamp"].notna()
        range_mask = (df["Timestamp"] < start_date) | (df["Timestamp"] > end_date)
        df.loc[valid_ts_mask & range_mask & (df["ERROR REASON"] == ""), "ERROR REASON"] = "Outside valid date range (2016-2026)"
        
        # If absolutely no timestamp related columns
        if not any(c in cols for c in ["Timestamp", "date", "time"]):
            df["ERROR REASON"] = "Missing timestamp column"

        return df

    def process(self, source, user_cols, filter_values, ts_hint, progress_cb=None):
        """Main processing pipeline with Dynamic Data Selection (filtering)."""
        all_processed = []
        
        def update_prog(msg, val=None):
            if progress_cb:
                progress_cb(msg, val)

        # 1. Load files (from folder or uploads)
        update_prog("Searching for files...", 5)
        # We'll do a quick scan to get total count for progress
        dfs = self.load_files(source)
        if not dfs:
            return None, None, None, "No data found in source."

        # 2. Pre-process each sheet
        total_dfs = len(dfs)
        for i, df in enumerate(dfs):
            pct = int(5 + (i/total_dfs)*45) # 5% to 50%
            update_prog(f"Normalizing sheet {i+1} of {total_dfs}...", pct)
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
        update_prog("Applying filters...", 60)
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
                # Flexible matching: if user gives '10:00', match any row containing it
                # We use the new standard 24-hour format for internal string comparison
                full_fmt = "%Y-%m-%d %H:%M:%S"
                
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

        # Final Formatting for Valid Timestamp (Standard 24-Hour Format)
        if not valid_df.empty:
            # Explicitly convert to datetime to avoid "Can only use .dt accessor with datetimelike values" errors
            valid_df["Timestamp"] = pd.to_datetime(valid_df["Timestamp"], errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")

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
            
            # --- Aggregation for Distinct Data (Targeted Sparse Merging) ---
            # We merge rows that are "compatible" (no conflicting non-null values).
            # Conflicting rows (e.g., 'rakesh' vs 'ravi') are kept separate.
            
            non_null_keys = valid_df[valid_df["_key"].notna()]
            null_keys = valid_df[valid_df["_key"].isna()]
            
            if not non_null_keys.empty:
                def merge_group(group):
                    # List of consolidated rows for this group
                    consolidated = []
                    
                    for _, row in group.iterrows():
                        merged = False
                        for i in range(len(consolidated)):
                            c_row = consolidated[i]
                            
                            # Check compatibility
                            conflict = False
                            check_cols = [c for c in group.columns if c not in ["WORKBOOK NAME", "SHEET NAME", "_key", "DUPLICATE"]]
                            for col in check_cols:
                                val1 = row[col]
                                val2 = c_row[col]
                                
                                if not pd.isna(val1) and not pd.isna(val2):
                                    if str(val1).strip() != "" and str(val2).strip() != "":
                                        if str(val1).strip().lower() != str(val2).strip().lower():
                                            conflict = True
                                            break
                            
                            if not conflict:
                                for col in group.columns:
                                    if col in ["WORKBOOK NAME", "SHEET NAME"]:
                                        existing_meta = set(str(v) for v in str(c_row[col]).split(", ") if v.strip())
                                        new_meta = str(row[col])
                                        existing_meta.add(new_meta)
                                        c_row[col] = ", ".join(sorted(existing_meta))
                                    elif pd.isna(c_row[col]) or str(c_row[col]).strip() == "":
                                        c_row[col] = row[col]
                                
                                consolidated[i] = c_row
                                merged = True
                                break
                        
                        if not merged:
                            new_row = row.copy()
                            consolidated.append(new_row)
                    
                    res_df = pd.DataFrame(consolidated)
                    # If the key-group resulted in multiple rows, mark them as 'Conflict'
                    if len(res_df) > 1:
                        res_df["Conflict"] = "Conflict"
                    else:
                        res_df["Conflict"] = ""
                    return res_df

                # Apply the merging logic per key-group
                update_prog("Analyzing conflicts & merging records...", 85)
                # We use include_groups=False to silence pandas FutureWarning
                distinct_non_null = non_null_keys.groupby("_key", group_keys=False).apply(merge_group, include_groups=False).reset_index(drop=True)
                update_prog("Finalizing reports...", 95)
                
            else:
                distinct_non_null = pd.DataFrame(columns=valid_df.columns.tolist() + ["Conflict"])

            distinct_df = pd.concat([distinct_non_null, null_keys], ignore_index=True)
            if "Conflict" not in distinct_df.columns: distinct_df["Conflict"] = ""
            distinct_df = distinct_df.drop(columns=["_key", "DUPLICATE"])
            
            valid_df = valid_df.drop(columns=["_key"])
        else:
            distinct_df = valid_df.copy()
            distinct_df["Conflict"] = ""

        # 8. Final Ordering
        def apply_final_order(df, is_invalid=False):
            if df.empty: return df
            for c in self.priority_columns:
                if c not in df.columns: df[c] = ""
            
            meta_cols = ["DUPLICATE", "WORKBOOK NAME", "SHEET NAME", "Conflict"]
            if is_invalid:
                # Add ISSUE column for invalid sheet
                df["ISSUE"] = df["ERROR REASON"].apply(
                    lambda x: "Functional Issue" if "range" in str(x).lower() else "Conversional Issue"
                )
                meta_cols = ["ISSUE", "WORKBOOK NAME", "SHEET NAME"]

            for c in meta_cols:
                if c not in df.columns: df[c] = ""
            
            cols = df.columns.tolist()
            priority = [c for c in self.priority_columns]
            metadata = [c for c in meta_cols]
            
            others = [c for c in cols if c not in priority and c not in metadata and c != "ERROR REASON"]
            
            final = priority + others
            if "ERROR REASON" in cols: final += ["ERROR REASON"]
            final += metadata
            return df[final]

        all_data = apply_final_order(valid_df)
        distinct_data = apply_final_order(distinct_df)
        invalid_data = apply_final_order(invalid_df, is_invalid=True)

        return all_data, distinct_data, invalid_data, None
