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
    col1_filter = st.sidebar.text_input("Filter Value for Col 1 (Optional)", placeholder="e.g. 20:04:2024")
    
    # Column 2
    col2_name = st.sidebar.text_input("Column 2 Name", value="Coin")
    col2_filter = st.sidebar.text_input("Filter Value for Col 2 (Optional)", placeholder="e.g. BTC")
    
    # Column 3
    col3_name = st.sidebar.text_input("Column 3 Name", value="Quantity")
    col3_filter = st.sidebar.text_input("Filter Value for Col 3 (Optional)")
    
    st.sidebar.markdown("#### 🕒 Timestamp Hint")
    ts_hint = st.sidebar.selectbox(
        "Format for Timestamp Filter",
        ["DD:MM:YYYY HH:MM:SS", "DD:MM:YYYY", "HH:MM", "HH:MM:SS"]
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
        
        # Handle listing files for preview
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
                st.dataframe(pd.DataFrame(file_info), use_container_width=True, height=200)
            
            with p_col2:
                st.write("**Detected Column Names (Global):**")
                sorted_cols = sorted(list(detected_cols))
                st.write(", ".join(sorted_cols))

        st.markdown("---")
        
        # Process Button
        if st.button("🚀 Process & Select Data"):
            user_cols = [col1_name, col2_name, col3_name]
            filter_values = [col1_filter, col2_filter, col3_filter]
            
            with st.spinner("Processing metadata and applying filters..."):
                all_data, distinct_data, invalid_data, error = processor.process(
                    source, user_cols, filter_values, ts_hint
                )
                
                if error:
                    st.error(error)
                else:
                    st.success("✅ Extraction complete!")
                    
                    # Metrics
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Selected Valid Rows", len(all_data))
                    m2.metric("Distinct Selected Rows", len(distinct_data))
                    m3.metric("Invalid Records", len(invalid_data))
                    
                    # Previews
                    tab1, tab2, tab3 = st.tabs(["ALLDATA", "DISTINCT DATA", "INVALID DATA"])
                    with tab1:
                        st.dataframe(all_data.head(100), use_container_width=True)
                    with tab2:
                        st.dataframe(distinct_data.head(100), use_container_width=True)
                    with tab3:
                        st.dataframe(invalid_data.head(100), use_container_width=True)
                    
                    # Generate Download
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        all_data.to_excel(writer, sheet_name='ALLDATA', index=False)
                        distinct_data.to_excel(writer, sheet_name='DISTINCT DATA', index=False)
                        invalid_data.to_excel(writer, sheet_name='INVALID DATA', index=False)
                    
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
