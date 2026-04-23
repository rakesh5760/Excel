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
    st.markdown("### Consolidate, Track (RID), and Process your Excel reports.")
    
    # 1. Action Selection
    action = st.radio("Select Action:", ["Create Raw Master", "Process Data"], horizontal=True)

    # Sidebar for Configuration
    st.sidebar.header("⚙️ Configuration")
    st.sidebar.markdown("#### 🔑 Duplicate Keys & Filters")
    col1_name = st.sidebar.text_input("Column 1 Name", value="Timestamp")
    col1_filter = st.sidebar.text_input("Filter Value for Col 1 (Optional)", placeholder="e.g. 2017-11-27")
    col2_name = st.sidebar.text_input("Column 2 Name", value="Coin")
    col2_filter = st.sidebar.text_input("Filter Value for Col 2 (Optional)", placeholder="e.g. BTC")
    col3_name = st.sidebar.text_input("Column 3 Name", value="Quantity")
    col3_filter = st.sidebar.text_input("Filter Value for Col 3 (Optional)")
    
    st.sidebar.markdown("#### 🕒 Timestamp Hint")
    ts_hint = st.sidebar.selectbox("Format for Timestamp Filter", ["YYYY-MM-DD HH:MM:SS", "YYYY-MM-DD", "HH:MM", "HH:MM:SS"])
    st.sidebar.markdown("---")
    st.sidebar.info("Rules: \n- One of the columns MUST be Timestamp.\n- RID tracks rows across all files.")

    # Main Area - Input Mode
    input_mode = st.radio("Select Input Mode:", ["File Upload", "Local Folder Path"], horizontal=True)
    source = None
    if input_mode == "File Upload":
        source = st.file_uploader("Upload Excel files (.xlsx)", type=["xlsx"], accept_multiple_files=True)
    else:
        source = st.text_input("Enter folder path:", placeholder="C:\\Users\\Data\\Excels")
        if source and not os.path.isdir(source):
            st.error("Invalid Directory Path.")
            source = None

    if source:
        processor = ExcelProcessor()
        
        # Pre-processing Preview
        st.subheader("🔍 Pre-Processing Preview")
        detected_cols = set()
        file_info = []
        files_to_preview = source if input_mode == "File Upload" else [os.path.join(source, f) for f in os.listdir(source) if f.endswith(".xlsx") and not f.startswith("~$")]
        
        if not files_to_preview:
            st.warning("No Excel files found.")
        else:
            with st.spinner("Analyzing files..."):
                for f in files_to_preview:
                    try:
                        # Reset pointer if it's a buffer
                        if hasattr(f, 'seek'): f.seek(0)
                        
                        name = os.path.basename(f) if isinstance(f, str) else f.name
                        with pd.ExcelFile(f, engine='openpyxl') as xls:
                            for sheet in xls.sheet_names:
                                df_h = pd.read_excel(xls, sheet_name=sheet, nrows=0)
                                detected_cols.update([str(c).strip() for c in df_h.columns])
                                file_info.append({"Workbook": name, "Sheet": sheet, "Cols": len(df_h.columns)})
                        
                        # Reset again after reading
                        if hasattr(f, 'seek'): f.seek(0)
                    except: pass
            
            p_col1, p_col2 = st.columns([1, 2])
            with p_col1: st.dataframe(pd.DataFrame(file_info), height=200)
            with p_col2: st.write(", ".join(sorted(list(detected_cols))))

        st.markdown("---")
        
        if action == "Create Raw Master":
            st.subheader("🛠️ Step 1: Create Raw Master")
            if st.button("🧶 Generate Raw Master"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                def ui_cb(msg, val):
                    status_text.text(msg); progress_bar.progress(val if val else 0)
                
                processor.load_files(source)
                raw_m, inv_m, error = processor.create_raw_master(progress_cb=ui_cb)
                
                if error: st.error(error)
                else:
                    # Persistence
                    st.session_state['master_df'] = raw_m
                    st.session_state['master_invalid'] = inv_m
                    st.session_state['master_zip'] = processor.get_updated_files_zip()
                    st.session_state['master_sheet_count'] = len(processor.raw_data)
                    st.success(f"✅ Raw Master Created! (Consolidated {st.session_state['master_sheet_count']} sheets)")

            # Display persistent results
            if 'master_df' in st.session_state:
                m_df = st.session_state['master_df']
                i_df = st.session_state['master_invalid']
                
                m_tab1, m_tab2 = st.tabs(["RAW MASTER (All Data)", "INVALID DATA"])
                with m_tab1: st.dataframe(m_df.head(100).astype(str), width='stretch')
                with m_tab2: st.dataframe(i_df.head(100).astype(str), width='stretch')

                m_c1, m_c2 = st.columns(2)
                with m_c1:
                    out_m = io.BytesIO()
                    with pd.ExcelWriter(out_m, engine='xlsxwriter') as wr: 
                        m_df.to_excel(wr, sheet_name='RawMaster', index=False)
                        i_df.to_excel(wr, sheet_name='InvalidData', index=False)
                    st.download_button("📥 Download RAW MASTER.xlsx", out_m.getvalue(), "RAW_MASTER.xlsx")
                with m_c2:
                    st.download_button("📦 Download UPDATED SOURCE FILES (ZIP)", st.session_state['master_zip'], "SOURCE_FILES_WITH_RID.zip")

        else: # Process Data
            st.subheader("📊 Step 2: Process & Consolidate")
            if st.button("🚀 Process & Select Data"):
                p_bar = st.progress(0)
                p_text = st.empty()
                def p_cb(msg, val):
                    p_text.text(msg); p_bar.progress(val if val else 0)
                
                all_d, dist_d, inv_d, err = processor.process(
                    source, [col1_name, col2_name, col3_name], 
                    [col1_filter, col2_filter, col3_filter], 
                    ts_hint, progress_cb=p_cb
                )
                
                if err: 
                    st.error(err)
                else:
                    st.session_state['all_data'] = all_d
                    st.session_state['distinct_data'] = dist_d
                    st.session_state['invalid_data'] = inv_d
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
