from navigation import make_sidebar
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
import os
import pandas as pd
import common_library.common_config as config
from common_library.common_snowflake import DataframeToSnowflake, ExecuteQuery
from common_library.common_reports import gather_reports, ReportConfig
import numpy as np
import pendulum
import time
import traceback
import uuid


def gen_url(row):
    if row.get("SMC_PATIENT_ID") is not None:
        return f"https://sm-foundation.inovalonone.com/patient/detail/{str(row['SMC_PATIENT_ID'])}"
    else:
        return "http://NO_PATIENT_ID_EXISTS_FOR_THIS_REPORT"


def BuildAggrid(data, read_only: bool, index_column: str):
    gb = GridOptionsBuilder.from_dataframe(data)

    if "pre_selected_rows" not in st.session_state:
        st.session_state.pre_selected_rows = []

    if read_only:
        gb.configure_default_column(editable=False)
    else:
        gb.configure_default_column(editable=True)
        gb.configure_selection(
            "multiple",
            pre_selected_rows=st.session_state.pre_selected_rows,
            use_checkbox=True,
        )

    gb.configure_column(
        "URL",
        headerName="SMC Link",
        cellRenderer=JsCode(
            """
            class UrlCellRenderer {
            init(params) {
                this.eGui = document.createElement('a');
                this.eGui.innerText = 'Open Patient';
                this.eGui.setAttribute('href', params.value);
                this.eGui.setAttribute('style', "text-decoration:none");
                this.eGui.setAttribute('target', "_blank");
            }
            getGui() {
                return this.eGui;
            }
            }
            """
        ),
        width=300,
    )

    gb.configure_column(index_column, headerCheckboxSelection=True)

    for column in data.columns:
        for prefix in config.report_exclude_column_prefixes:
            if column.startswith(prefix):
                gb.configure_column(column, hide=True)

    go = gb.build()

    return AgGrid(
        data,
        gridOptions=go,
        allow_unsafe_jscode=True,
        height=600,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.MANUAL | GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED | GridUpdateMode.MODEL_CHANGED,
    )


def get_file_data(report_data_query: str, report_object: ReportConfig):
    try:
        conn = st.connection("snowflake")

        temp_df = conn.query(report_data_query, ttl=600).astype(str)
        temp_df.replace([np.nan, "None", "nan", "NaN", "nat", "NaT", ""], None, inplace=True)

        if report_object.mfr_reference_id:
            temp_df["MFR_ID"] = temp_df[report_object.mfr_reference_id]
            mfr_id_column = temp_df.pop("MFR_ID")
            temp_df.insert(0, "MFR_ID", mfr_id_column)
            temp_df.set_index("MFR_ID")
        else:
            temp_df.reset_index(names=["Row"], inplace=True)
            temp_df["Row"] = temp_df["Row"] + 1  # Incrementing by 1 to "skip" the header row in a file

        temp_df["URL"] = temp_df.apply(gen_url, axis=1)
        url_column = temp_df.pop("URL")
        temp_df.insert(1, "URL", url_column)

        temp_df = temp_df.replace({pd.NaT: None})

        # for column in temp_df.columns:
        #     for prefix in config.report_exclude_column_prefixes:
        #         if column.startswith(prefix):
        #             temp_df = temp_df.drop(columns=[column])

        st.session_state.file_data_df = temp_df.copy()
        del temp_df
    except Exception as e:
        st.error(traceback.format_exc())
        raise e


def submit_data_for_approval(target_report_database, full_fixed_data_df, target_report_object: ReportConfig, original_filename, submitted_by, restatement: bool):
    st.toast("Submitting Corrections to Database...")
    ddl_tokens = {}
    ddl_tokens["%TARGET_DATABASE%"] = target_report_database
    ddl_tokens["%TARGET_SCHEMA%"] = target_report_object.target_schema
    ddl_tokens["%TARGET_TABLE%"] = f"{target_report_object.target_table}_CORRECTIONS"

    final_table_ddl = target_report_object.target_table_ddl
    for token, value in ddl_tokens.items():
        final_table_ddl = final_table_ddl.replace(token, value)

    corrections_id = str(uuid.uuid4())

    full_fixed_data_df["REPORT_RUN_ID"] = corrections_id

    DataframeToSnowflake(
        target_database=target_report_database,
        target_schema=target_report_object.target_schema,
        target_table=f"{target_report_object.target_table}_CORRECTIONS",
        primary_key="REPORT_RECORD_ID",
        dataframe=full_fixed_data_df,
        operation="upsert",
        include_metrics=False,
        target_ddl=final_table_ddl,
    )

    st.toast("Submitting Approval Request...")
    insert_query = f"""
INSERT INTO {config.reporting_database}.METADATA.FILE_EXPORT_APPROVALS (REPORT_DATABASE, REPORT_SCHEMA, REPORT_TABLE, REPORT_RUN_ID, ORIGINAL_FILENAME, SUBMITTED_BY, SUBMITTED_ON, RESTATEMENT_FILE)
SELECT '{target_report_database}' as REPORT_DATABASE
    ,'{target_report_object.target_schema}' as REPORT_SCHEMA
    ,'{target_report_object.target_table}' as REPORT_TABLE
    ,'{corrections_id}' as REPORT_RUN_ID
    ,'{original_filename}' as ORIGINAL_FILENAME
    ,'{submitted_by}' as SUBMITTED_BY
    ,'{pendulum.now()}' as SUBMITTED_ON
    {",TRUE as RESTATEMENT_FILE" if restatement else ",FALSE as RESTATEMENT_FILE"}
    """
    result = ExecuteQuery(
        sql_query=insert_query,
        database=target_report_database,
        schema=target_report_object.target_schema,
    )

    if result[0][0] == 1:
        st.toast("Approval Submitted Successfully")
    else:
        st.toast("Approval DID NOT SUBMIT Successfully.  Please reach out to the Data Team for help.")
    time.sleep(0.5)


def init_globals():
    global show_file_metadata
    show_file_metadata = False

    global show_export_preview
    show_export_preview = False

    global show_exported_files
    show_exported_files = False

    global read_only
    read_only = False

    global exported_files
    exported_files = []

    global selected_report
    selected_report = None

    global exports_df
    exports_df = None

    global report_data_query
    report_data_query = None

    # Capture the user_info
    global user_info
    user_info = st.session_state.get("user_info")

    global debug
    if str(user_info["nickname"]).lower() in ["tim.bytnar"]:
        debug = True
    else:
        debug = False

    # Establish connection to Snowflake
    global conn
    conn = st.connection("snowflake")

    global mfr_reports
    all_reports = gather_reports("/data_pipeline_airflow/dags/Reporting/Reports/")
    mfr_reports = [
        x for x in all_reports if x.recipient.lower() in ["example1", "example2", "example3"]
    ]


st.set_page_config(
    page_title="Manufacturer Reports",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize all global variables
init_globals()

# Custom CSS Markdown
st.markdown(
    """
    <style>
        div[data-testid=stToastContainer] {
            padding: 1% 4% 65% 2%;
            align-items: center;
        }

        div[data-testid=stToast] {
            padding: 20px 10px 40px 10px;
            width: 20%;
            text-align: center;
        }

        [data-testid=stToastContainer] [data-testid=stMarkdownContainer] > p {
            font-size: 20px; font-style: normal; font-weight: 400;
            foreground-color: #ffffff;
            text-align: center;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

###############################################################################################
## HTML BODY
###############################################################################################

# Build and display the sidebar navigation
make_sidebar()


# Get all report configs.  NOTE: This depends on the data_pipeline_airflow repo existing in the same parent folder as streamlit
reports_parent_path = os.path.join(os.path.abspath(""), "data_pipeline_airflow", "dags", "Reporting", "Reports")
all_reports = gather_reports(reports_parent_path)


###############################################################################################
## Manufacturer Reports Section
###############################################################################################

st.header("Manufacturer Reports")

# Setup 3-column layout
col1, col2, col3 = st.columns(3)

# Init Empty Objects
report_select = None
file_select = None
report_data_query = None


# MFR Selection
mfr_list = sorted(list(set([x.recipient for x in mfr_reports])))
mfr_list.insert(0, "Choose...")
mfr_select = col1.selectbox("Manufacturer", mfr_list)


# Report Selection
if mfr_select != "Choose...":
    report_list = [x.agreement_name for x in mfr_reports if x.recipient.lower() == mfr_select.lower()]
    report_list.insert(0, "Choose...")
    report_select = col2.selectbox("Report", report_list)
    for report in mfr_reports:
        if report.recipient.lower() == mfr_select.lower() and report.agreement_name.lower() == report_select.replace("_", " ").lower():
            selected_report = report

    if selected_report and selected_report.remediation_location in ["concert_ai"]:
        read_only = True

# File Selection
if report_select and report_select != "Choose...":
    exports_query = f"""
        SELECT * FROM {config.reporting_database}.METADATA.FILE_EXPORTS 
        WHERE REPORT_SCHEMA ILIKE '{mfr_select}%'
        AND REPORT_TABLE ILIKE '%{selected_report.target_table}%'
        ORDER BY EXPORTED_DATE DESC
    """
    exports_df = conn.query(exports_query, ttl=600)

    file_df = exports_df[exports_df["REPORT_TABLE"] == selected_report.target_table]
    file_list = list(file_df["EXPORTED_FILE_NAME"])
    file_list.insert(0, "Choose...")
    file_select = col3.selectbox("File", file_list)


if file_select and file_select != "Choose...":
    export_record = file_df[file_df["EXPORTED_FILE_NAME"] == file_select]
    export_record = export_record.replace(np.nan, None)
    report_run_id = export_record.iloc[0]["REPORT_RUN_ID"]
    report_database = export_record.iloc[0]["REPORT_DATABASE"]
    report_schema = export_record.iloc[0]["REPORT_SCHEMA"]
    report_table = export_record.iloc[0]["REPORT_TABLE"]
    report_exported_date = export_record.iloc[0]["EXPORTED_DATE"]
    report_submitted_date = export_record.iloc[0]["SUBMITTED_DATE"]
    report_corrected_date = export_record.iloc[0]["CORRECTED_DATE"]
    report_corrected_by = export_record.iloc[0]["CORRECTED_BY"]

    # Match up the report record to the report config
    report: ReportConfig
    for report in all_reports:
        if config.reporting_database == report_database and report.target_table == report_table and report.target_schema == report_schema:
            report_object: ReportConfig = report

    report_data_query = f"""SELECT * FROM {report_database}.{report_schema}.{report_table}
                            WHERE REPORT_RUN_ID = '{report_run_id}'
    """

    show_file_metadata = True

st.divider()

###############################################################################################
## Exported Data Section
###############################################################################################
if show_file_metadata:
    if read_only:
        st.header("Report Data - READ ONLY")
        st.write(f"Data edits for this report are performed at: {selected_report.remediation_location.upper()}")
    else:
        st.header("Report Data")

    mf_data = [{"Report Run ID": report_run_id, "Exported Date": report_exported_date, "Corrected Date": report_corrected_date, "Corrected By": report_corrected_by}]

    get_file_data(report_data_query, report_object=selected_report)

    if selected_report.mfr_reference_id:
        index_column = "MFR_ID"
    else:
        index_column = "Row"

    grid_return = BuildAggrid(st.session_state.file_data_df, read_only, index_column)
    if str(user_info["nickname"]).lower() in [
        "tim.bytnar",
    ]:
        is_restatement = st.checkbox(label="Restatement?", value=False)
    else:
        is_restatement = False

    if grid_return.selected_data is not None or is_restatement:
        if is_restatement:
            report_data_to_update = st.session_state.file_data_df
        else:
            report_data_to_update = grid_return.selected_data
        report_data_to_update["SOURCE_DATA"] = file_select
        report_data_to_update["UPDATED_ON"] = str(pendulum.now())
        report_data_to_update["UPDATED_BY"] = str(user_info["nickname"]).lower()

        if selected_report.mfr_reference_id:
            report_data_to_update = report_data_to_update.drop(columns=["MFR_ID"])
        else:
            report_data_to_update = report_data_to_update.drop(columns=["Row"])
        report_data_to_update = report_data_to_update.drop(columns=["URL"])
        report_data_to_update = report_data_to_update.reset_index(drop=True)

        preview_data = report_data_to_update.copy()
        preview_data = preview_data.astype(str).replace("nan", "")
        preview_data.replace([np.nan, "None", "nan", "NaN", "nat", "NaT", ""], None, inplace=True)

        for column in report_data_to_update.columns:
            for prefix in config.report_exclude_column_prefixes:
                if column.startswith(prefix):
                    preview_data = preview_data.drop(columns=[column])

        if len(preview_data) > 0:
            show_export_preview = True

    st.divider()

###############################################################################################
## Fix Data Preview Section
###############################################################################################
if show_export_preview:
    st.header("Fix Data Preview")
    st.write(preview_data)
    if st.button("Submit for Approval"):
        with st.spinner("Wait for it...", show_time=True):
            submit_data_for_approval(
                target_report_database=report_database,
                full_fixed_data_df=report_data_to_update,
                target_report_object=selected_report,
                original_filename=file_select,
                submitted_by=str(user_info["nickname"]).lower(),
                restatement=is_restatement,
            )

            st.cache_data.clear()
            get_file_data(report_data_query=report_data_query, report_object=selected_report)
            st.rerun()


###############################################################################################
## DEBUG Section
###############################################################################################

if debug:
    st.divider()
    st.title("DEBUG INFORMATION")
    if "file_data_df" in st.session_state:
        st.write(f"File Data DF Exists with Record Count: {len(st.session_state.file_data_df)}")

        st.header("DEBUG: SELECTED REPORT")
        st.write(selected_report)

        st.header("DEBUG: ALL EXPORTED FILES")
        st.write(exports_df)

        st.header("DEBUG: REPORT DATA QUERY")
        st.write(report_data_query)

        if selected_report and selected_report.mfr_reference_id:
            st.write("DEBUG: Row will be added to dataframe.")
