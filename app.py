import streamlit as st
import pandas as pd
from processor import ExcelProcessor
import io
import os

# Page Configuration
st.set_page_config(page_title="Excel Data Processor & Consolidator", layout="wide")

st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
        background-color: #ff4b4b;
        color: white;
    }
    .stDownloadButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
        background-color: #2e7bcf;
        color: white;
    }
    .filter-box {
        padding: 10px;
        border: 1px solid #333;
        border-radius: 5px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.title("📊 Excel Data Processor & Consolidator")
    st.markdown("### Process, Normalize, and Consolidate your Excel reports with Dynamic Selection.")
    
    # Sidebar for Configuration
    st.sidebar.header("⚙️ Configuration")
    
    st.sidebar.markdown("#### 🔑 Duplicate Keys & Filters")
    
    # Column 1
    col1_name = st.sidebar.text_input("Column 1 Name", value="Timestamp")
    col1_filter = st.sidebar.text_input("Filter Value for Col 1 (Optional)", placeholder="e.g. 2017-11-27")
    
    # Column 2
    col2_name = st.sidebar.text_input("Column 2 Name", value="Coin")
    col2_filter = st.sidebar.text_input("Filter Value for Col 2 (Optional)", placeholder="e.g. BTC")
    
    # Column 3
    col3_name = st.sidebar.text_input("Column 3 Name", value="Quantity")
    col3_filter = st.sidebar.text_input("Filter Value for Col 3 (Optional)")
    
    st.sidebar.markdown("#### 🕒 Timestamp Hint")
    ts_hint = st.sidebar.selectbox(
        "Format for Timestamp Filter",
        ["YYYY-MM-DD HH:MM:SS", "YYYY-MM-DD", "HH:MM", "HH:MM:SS"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Rules:**
    - One of the 3 columns MUST map to Timestamp.
    - WORKBOOK NAME and SHEET NAME are preserved.
    - Empty filter values will include all data for that column.
    """)

    # Main Area - Input Mode
    input_mode = st.radio("Select Input Mode:", ["File Upload", "Local Folder Path"], horizontal=True)
    
    source = None
    if input_mode == "File Upload":
        source = st.file_uploader(
            "Upload multiple Excel files (.xlsx)",
            type=["xlsx"],
            accept_multiple_files=True
        )
    else:
        source = st.text_input("Enter the full path to your folder containing Excel files:", placeholder="C:\\Users\\Data\\Excels")
        if source and not os.path.isdir(source):
            st.error("Invalid Directory: Provided path does not exist or is not a folder.")
            source = None

    if source:
        processor = ExcelProcessor()
        
        # Pre-processing Preview
        st.subheader("🔍 Pre-Processing Preview")
        detected_cols = set()
        file_info = []
        
        files_to_preview = []
        if input_mode == "File Upload":
            files_to_preview = source
        else:
            if os.path.isdir(source):
                for f in os.listdir(source):
                    if f.endswith(".xlsx") and not f.startswith("~$"):
                        files_to_preview.append(os.path.join(source, f))
        
        if not files_to_preview:
            st.warning("No Excel files found to preview.")
        else:
            with st.spinner("Analyzing files..."):
                for f in files_to_preview:
                    try:
                        name = os.path.basename(f) if isinstance(f, str) else f.name
                        xls = pd.ExcelFile(f)
                        for sheet in xls.sheet_names:
                            df_head = pd.read_excel(xls, sheet_name=sheet, nrows=0)
                            detected_cols.update([str(c).strip() for c in df_head.columns])
                            file_info.append({"Workbook": name, "Sheet": sheet, "Cols": len(df_head.columns)})
                    except Exception as e:
                        st.error(f"Error reading {name}: {e}")
            
            p_col1, p_col2 = st.columns([1, 2])
            with p_col1:
                st.write("**Detected Workbooks & Sheets:**")
                st.dataframe(pd.DataFrame(file_info), width='stretch', height=200)
            
            with p_col2:
                st.write("**Detected Column Names (Global):**")
                sorted_cols = sorted(list(detected_cols))
                st.write(", ".join(sorted_cols))

        st.markdown("---")
        
        # Process Button
        if st.button("🚀 Process & Select Data"):
            user_cols = [col1_name, col2_name, col3_name]
            filter_values = [col1_filter, col2_filter, col3_filter]
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def my_cb(msg, val):
                status_text.text(msg)
                if val is not None:
                    progress_bar.progress(val)

            with st.spinner("Initializing..."):
                all_data, distinct_data, invalid_data, error = processor.process(
                    source, user_cols, filter_values, ts_hint, progress_cb=my_cb
                )
                
                # Cleanup progress
                progress_bar.empty()
                status_text.empty()
                
                if error:
                    st.error(error)
                else:
                    # Log any skipped files
                    if processor.load_errors:
                        for skip_err in processor.load_errors:
                            st.warning(f"⚠️ {skip_err}")

                    if all_data.empty:
                        st.info("ℹ️ No rows matched your selected filters or columns. Please check your configuration.")
                        # Clear previous data from state to prevent showing old results
                        if 'all_data' in st.session_state: del st.session_state['all_data']
                    else:
                        st.session_state['all_data'] = all_data
                        st.session_state['distinct_data'] = distinct_data
                        st.session_state['invalid_data'] = invalid_data
                        st.success("✅ Extraction complete!")

        # Show results and Column Merger if data exists in session state
        if 'all_data' in st.session_state and not st.session_state['all_data'].empty:
            all_df = st.session_state['all_data']
            distinct_df = st.session_state['distinct_data']
            invalid_df = st.session_state['invalid_data']

            # Metrics
            m1, m2, m3 = st.columns(3)
            m1.metric("Selected Valid Rows", len(all_df))
            m2.metric("Distinct Selected Rows", len(distinct_df))
            m3.metric("Invalid Records", len(invalid_df))
            
            # --- 🛠️ COLUMN MERGER SECTION ---
            st.markdown("---")
            with st.expander("🛠️ Column Merger (Combine two columns vertically)", expanded=True):
                st.info("Select two columns below and click 'Combine' to merge them into a new 'Merged Column'.")
                available_cols = all_df.columns.tolist()
                
                # Using a FORM to prevent the UI from "closing" or rerunning on every individual radio click
                with st.form("merger_form"):
                    mc1, mc2 = st.columns(2)
                    with mc1:
                        col_a = st.radio("Select Column A", options=available_cols, key="merge_a_radio")
                    with mc2:
                        col_b = st.radio("Select Column B", options=available_cols, key="merge_b_radio")
                    
                    submit_merge = st.form_submit_button("🧪 Combine Selected Columns")
                
                if submit_merge:
                    if col_a == col_b:
                        st.error("Please select two DIFFERENT columns to merge.")
                    else:
                        with st.spinner("Merging columns..."):
                            def merge_logic(df):
                                if col_a in df.columns and col_b in df.columns:
                                    # Create the merged column with newline
                                    df["Merged Column"] = df[col_a].astype(str) + "\n" + df[col_b].astype(str)
                                    # Clean up "nan" strings if any
                                    df["Merged Column"] = df["Merged Column"].str.replace("nan", "").str.strip()
                                    # Drop originals
                                    df = df.drop(columns=[col_a, col_b])
                                    # Move Merged Column after 'INR' column if it exists
                                    cols = df.columns.tolist()
                                    target_idx = 0
                                    if "INR" in cols:
                                        target_idx = cols.index("INR") + 1
                                    
                                    cols.insert(target_idx, cols.pop(cols.index("Merged Column")))
                                    df = df[cols]
                                return df
                            
                            st.session_state['all_data'] = merge_logic(all_df)
                            st.session_state['distinct_data'] = merge_logic(distinct_df)
                            st.rerun()

            # Previews
            # We cast to string for the preview to avoid Arrow serialization errors with mixed types
            tab1, tab2, tab3 = st.tabs(["ALLDATA", "DISTINCT DATA", "INVALID DATA"])
            with tab1:
                st.dataframe(st.session_state['all_data'].head(100).astype(str), width='stretch')
            with tab2:
                st.dataframe(st.session_state['distinct_data'].head(100).astype(str), width='stretch')
            with tab3:
                st.dataframe(st.session_state['invalid_data'].head(100).astype(str), width='stretch')
            
            # Generate Download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state['all_data'].to_excel(writer, sheet_name='ALLDATA', index=False)
                st.session_state['distinct_data'].to_excel(writer, sheet_name='DISTINCT DATA', index=False)
                st.session_state['invalid_data'].to_excel(writer, sheet_name='INVALID DATA', index=False)
                
                # Apply Wrap Text to "Merged Column" if it exists
                workbook = writer.book
                wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                
                for sheet_name in ['ALLDATA', 'DISTINCT DATA']:
                    worksheet = writer.sheets[sheet_name]
                    df_to_check = st.session_state['all_data'] if sheet_name == 'ALLDATA' else st.session_state['distinct_data']
                    if "Merged Column" in df_to_check.columns:
                        col_idx = df_to_check.columns.get_loc("Merged Column")
                        # Apply to the whole column (excluding header usually done by pandas, but we set for all data rows)
                        worksheet.set_column(col_idx, col_idx, 20, wrap_format)

            st.download_button(
                label="📥 Download PROCESSED DATA.xlsx",
                data=output.getvalue(),
                file_name="PROCESSED DATA.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    else:
        st.info("Please upload files or provide a folder path to start.")

if __name__ == "__main__":
    main()
