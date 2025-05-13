from navigation import make_sidebar
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
import os
import pandas as pd
import common_library.common_config as config
from common_library.common_snowflake import DataframeToSnowflake, ExecuteQuery
from common_library.common_reports import gather_reports, ReportConfig, submit_report
from common_library.common_aws import UploadFileToS3
import numpy as np
import pendulum
import re
import traceback
from utilities import log_to_console

st.set_page_config(
    page_title="Manufacturer Reports - Approvals",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init_globals():
    global cd_grid_return
    cd_grid_return = None

    global uc_grid_return
    uc_grid_return = None

    global approved_on
    approved_on = None

    # Capture the user_info
    global user_info
    user_info = st.session_state.get("user_info")

    global debug
    if str(user_info["nickname"]).lower() in ["tim.bytnar"]:
        debug = True
    else:
        debug = False

    global is_restatement
    is_restatement = False

    # Establish connection to Snowflake
    global conn
    conn = st.connection("snowflake")

    global mfr_reports
    all_reports = gather_reports("/example_data_pipeline_airflow/dags/Reporting/Reports/")
    mfr_reports = [
        x for x in all_reports if x.recipient.lower() in ["example1", "example2", "example3"]
    ]


def gen_url(row):
    if row.get("SMC_PATIENT_ID") is not None:
        return f"https://customer_management_system/patient/detail/{str(row['SMC_PATIENT_ID'])}"
    else:
        return "http://NO_PATIENT_ID_EXISTS_FOR_THIS_REPORT"


def BuildApprovalsAggrid(data):
    gb = GridOptionsBuilder.from_dataframe(data)

    if "pre_selected_rows" not in st.session_state:
        st.session_state.pre_selected_rows = []

    gb.configure_default_column(editable=False)
    gb.configure_selection(
        "single",
        pre_selected_rows=st.session_state.pre_selected_rows,
        use_checkbox=True,
    )
    go = gb.build()

    return AgGrid(
        data,
        gridOptions=go,
        allow_unsafe_jscode=True,
        height=200,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.MANUAL | GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED | GridUpdateMode.MODEL_CHANGED,
    )


def BuildCorrectionsAggrid(data, read_only: bool):
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

    gb.configure_column("Row", headerCheckboxSelection=True)

    go = gb.build()

    return AgGrid(
        data,
        gridOptions=go,
        allow_unsafe_jscode=True,
        height=300,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.MANUAL | GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED | GridUpdateMode.MODEL_CHANGED,
    )


def refresh_approvals_data():
    try:
        conn = st.connection("snowflake")
        approvals_query = f"""
        SELECT *
        FROM {config.reporting_database}.METADATA.FILE_EXPORT_APPROVALS
        WHERE DELETED = FALSE
        ORDER BY SUBMITTED_ON DESC
        """
        temp_df = conn.query(approvals_query, ttl=600).astype(str).reset_index(names=["Row"])
        temp_df.replace([np.nan, "None", "nan", "NaN", "nat", "NaT", ""], None, inplace=True)

        st.session_state.unapproved_submissions = temp_df.copy()
        del temp_df
    except Exception as e:
        st.error(traceback.format_exc())
        raise e


def display_post_approval_buttons(final_data: pd.DataFrame, report_object: ReportConfig, report_run_id, is_restatement: bool):
    split_filename = original_filename.split(".")
    filename = str(split_filename[0])
    file_ext = str(split_filename[1])
    match = re.search(r"V(\d+)$", filename)
    if match:
        version = int(match.group(1)) + 1
        filename = re.sub(r"V\d+$", f"V{version}", filename)
    else:
        filename += "_V1"
    fix_filename = filename + "." + file_ext

    row2 = st.columns([0.5, 0.9], gap="small", vertical_alignment="center")

    fix_filename_input = row2[0].text_input(label="File Name: ", value=fix_filename)

    row3 = st.columns([0.11, 0.2, 1.1])

    primary_destination = report_object.destinations[0]
    if primary_destination.formatter.type == "csv":
        from common_library.Reporting.formatters import format_csv

        format_csv(df=final_data, filename=fix_filename_input, destination=primary_destination)
    elif primary_destination.formatter.type == "excel":
        from common_library.Reporting.formatters import format_excel

        format_excel(df=final_data, filename=fix_filename_input, destination=primary_destination)
    elif primary_destination.formatter.type == "bcbst_copay":
        from common_library.Reporting.formatters import format_bcbst_copay

        format_bcbst_copay(df=final_data, filename=fix_filename_input, destination=primary_destination)

    with open(os.path.join(config.working_directory, fix_filename_input)) as file:
        row3[0].download_button(
            label="Download",
            data=file,
            file_name=fix_filename_input,
            mime="text/csv",
        )

    with row3[1].popover("Submit to MFR"):
        st.markdown("Are you sure you wish to submit these corrections to the Manufacturer? 🚨")
        if st.button("Confirm", key="corrections_submission_btn", type="primary"):
            s3_file_key = f"{primary_destination.formatter.carboncopy_path.lower()}{fix_filename_input}"
            log_to_console(s3_file_key)
            UploadFileToS3(os.path.join(config.working_directory, fix_filename_input), bucket=config.reporting_s3_bucket, object_key=s3_file_key)
            st.toast(f"{fix_filename_input} sent to S3.")

            if config.reporting_database == "TEST_REPORTS_CORE":
                test_skip = True
            else:
                test_skip = False

            submit_report(report_object=report_object, destination=primary_destination, output_filename=fix_filename_input, global_skip=test_skip)
            if is_restatement is not True:  # Dont judge me for this
                file_exports_query = f"""
                    INSERT INTO {config.reporting_database}.METADATA.FILE_EXPORTS (REPORT_DATABASE, REPORT_SCHEMA, REPORT_TABLE, example_REPORT_RUN_ID, EXPORTED_FILE_NAME, EXPORTED_DATE, SUBMITTED_DATE)
                    SELECT '{config.reporting_database}', '{report_object.target_schema}', '{report_object.target_table}', '{report_run_id}', '{fix_filename_input}', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) 
                """
                result_1 = ExecuteQuery(sql_query=file_exports_query, database=config.reporting_database, schema="METADATA")

                complete_query = f"""
                UPDATE {config.reporting_database}.METADATA.FILE_EXPORT_APPROVALS
                SET COMPLETED_ON = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP())
                WHERE example_REPORT_RUN_ID = '{example_report_run_id}'
                AND SUBMITTED_ON::STRING = '{submitted_on}'
                """
                result_2 = ExecuteQuery(sql_query=complete_query, database=config.reporting_database, schema="METADATA")

                if result_1[0][0] == 1 and result_2[0][0] == 1:
                    st.toast(f"{fix_filename_input} submitted to MFR Successfully.")
                else:
                    st.toast(f"{fix_filename_input} DID NOT SUBMIT Successfully.  Please reach out to the Data Team for help.")

                st.cache_data.clear()
                refresh_approvals_data()
                st.rerun()


# Initialize all global variables
init_globals()

# Build and display the sidebar navigation
make_sidebar()

###############################################################################################
## Correction Submissions Section
###############################################################################################

with st.container(border=True) as corrections:
    st.header("Corrections Submissions")
    refresh_approvals_data()
    uc_grid_return = BuildApprovalsAggrid(st.session_state["unapproved_submissions"])

    row1 = st.columns([0.2, 0.2, 1.1])

    if row1[0].button("Refresh Submissions", key="submissions_refresh_btn"):
        st.cache_data.clear()
        refresh_approvals_data()
        st.rerun()

    if uc_grid_return.selected_data is not None:
        submission_record = uc_grid_return.selected_data
        submission_record = submission_record.replace({np.nan, None})
        if pd.isna(submission_record.iloc[0]["APPROVED_ON"]):
            approved_on = None
        else:
            approved_on = submission_record.iloc[0]["APPROVED_ON"]

        report: ReportConfig
        for report in mfr_reports:
            if (
                report.target_schema == submission_record.iloc[0]["REPORT_SCHEMA"]
                and report.target_table == str(submission_record.iloc[0]["REPORT_TABLE"]).replace("_CORRECTIONS", "")
                and submission_record.iloc[0]["REPORT_DATABASE"] == config.reporting_database
            ):
                selected_report = report

        example_report_run_id = submission_record.iloc[0]["example_REPORT_RUN_ID"]
        submitted_on = submission_record.iloc[0]["SUBMITTED_ON"]
        submitted_on = submitted_on.replace("T", " ")

        with row1[1].popover("Reject Submission"):
            st.markdown("Are you sure you wish to reject this submission? 🚨")
            if st.button("Confirm", key="submissions_delete_btn", type="primary"):
                delete_query = f"""
                UPDATE {config.reporting_database}.METADATA.FILE_EXPORT_APPROVALS
                SET DELETED = TRUE
                WHERE example_REPORT_RUN_ID = '{example_report_run_id}'
                AND SUBMITTED_ON::STRING = '{submitted_on}'
                """

                result = ExecuteQuery(sql_query=delete_query, database=config.reporting_database, schema="METADATA")

                if result[0][0] > 0:
                    st.toast("Submission rejected successfully")
                else:
                    st.toast("Submission could not be marked as rejected.  Reach out to the data team for help.")

                st.cache_data.clear()
                refresh_approvals_data()
                st.rerun()

###############################################################################################
## Corrections Data Section
###############################################################################################
if uc_grid_return and uc_grid_return.selected_data is not None:
    with st.container(border=True):
        submission_record = uc_grid_return.selected_data
        report_target_database = submission_record.iloc[0]["REPORT_DATABASE"]
        report_target_schema = submission_record.iloc[0]["REPORT_SCHEMA"]
        report_target_table = submission_record.iloc[0]["REPORT_TABLE"]
        example_report_run_id = submission_record.iloc[0]["example_REPORT_RUN_ID"]
        submitted_on = submission_record.iloc[0]["SUBMITTED_ON"]
        original_filename = submission_record.iloc[0]["ORIGINAL_FILENAME"]
        is_restatement = submission_record.iloc[0]["RESTATEMENT_FILE"]

        corrections_query = f"""
    SELECT *
    FROM {report_target_database}.{report_target_schema}.{report_target_table + "_CORRECTIONS"}
    WHERE example_REPORT_RUN_ID = '{example_report_run_id}'
    """
        corrections_data = conn.query(sql=corrections_query).reset_index(names=["Row"])

        corrections_data["Row"] = corrections_data["Row"] + 1  # Incrementing by 1 to "skip" the header row in a file
        # corrections_data["URL"] = corrections_data.apply(gen_url, axis=1)
        corrections_data["URL"] = None
        corrections_data = corrections_data.replace({pd.NaT: None})

        # Move the URL column to the first position after the index
        url_column = corrections_data.pop("URL")
        corrections_data.insert(1, "URL", url_column)

        if not approved_on and approved_on == approved_on:
            st.header("Corrections Data")
            cd_grid_return = BuildCorrectionsAggrid(corrections_data, read_only=False)
        else:
            st.header(f"Corrections Data - Approved On : {approved_on}")
            cd_grid_return = BuildCorrectionsAggrid(corrections_data, read_only=True)

            for column in corrections_data.columns:
                for prefix in config.report_exclude_column_prefixes:
                    if column.startswith(prefix):
                        corrections_data = corrections_data.drop(columns=[column])

            corrections_data = corrections_data.drop(columns=["Row"])
            corrections_data = corrections_data.drop(columns=["URL"])
            corrections_data = corrections_data.reset_index(drop=True)
            display_post_approval_buttons(final_data=corrections_data, report_object=selected_report, report_run_id=example_report_run_id, is_restatement=is_restatement)

###############################################################################################
## Fix Data Preview Section
###############################################################################################
if cd_grid_return and (cd_grid_return.selected_data is not None or (is_restatement is True and not approved_on)):
    with st.container(border=True):
        if is_restatement is True:
            report_data_to_update = corrections_data
        else:
            report_data_to_update = cd_grid_return.selected_data
        report_data_to_update = report_data_to_update.drop(columns=["Row"])
        report_data_to_update = report_data_to_update.drop(columns=["URL"])
        report_data_to_update = report_data_to_update.reset_index(drop=True)

        preview_data = report_data_to_update.copy()
        for column in report_data_to_update.columns:
            for prefix in config.report_exclude_column_prefixes:
                if column.startswith(prefix):
                    preview_data = preview_data.drop(columns=[column])

        st.header("Fix Data Preview")
        st.write(preview_data)

        with st.popover("Approve Corrections"):
            st.markdown("Are you sure you wish to submit these corrections to the Manufacturer? 🚨")
            if st.button("Confirm", key="approve_corrections_btn"):
                approval_query = f"""
                UPDATE {config.reporting_database}.METADATA.FILE_EXPORT_APPROVALS
                SET APPROVED_BY = '{str(user_info["nickname"]).lower()}'
                ,APPROVED_ON = '{pendulum.now()}'
                WHERE example_REPORT_RUN_ID = '{example_report_run_id}'
                """
                st.toast("Marking submission as approved.")
                result = ExecuteQuery(sql_query=approval_query, database=config.reporting_database, schema="METADATA")
                if result[0][0] == 1:
                    st.toast("Submission marked as approved.")
                else:
                    st.toast("Submission COULD NOT be marked as approved.  Reach out to the data team for help.")

                if is_restatement is not True:
                    st.toast("Appending corrections data to database.")
                    DataframeToSnowflake(
                        target_database=config.reporting_database,
                        target_schema=selected_report.target_schema,
                        target_table=selected_report.target_table,
                        primary_key=None,
                        operation="append",
                        dataframe=report_data_to_update,
                        ignore_autoincrementing=False,
                    )
                    st.toast("Corrections data appended successfully.")

                st.cache_data.clear()
                refresh_approvals_data()
                st.rerun()

        # if approved_on:
        #     display_post_approval_buttons(final_data=corrections_data, report_object=selected_report, report_run_id=example_report_run_id)


###############################################################################################
## DEBUG Section
###############################################################################################

if debug:
    st.divider()
    st.title("DEBUG INFORMATION")

    if selected_report:
        st.header("DEBUG: SELECTED REPORT")
        st.write(selected_report)

    st.header("DEBUG: ALL UNAPPROVED SUBMISSIONS")
    st.write(st.session_state.unapproved_submissions)

    st.header("DEBUG: CD_GRID_EXISTS?")
    st.write("YES" if cd_grid_return else "NO")

    st.header("DEBUG: Is Restatement?")
    st.write(is_restatement if is_restatement is True else "NO")
    st.write(is_restatement == True)

    st.header("DEBUG: Approved_On Set?")
    st.write(approved_on if approved_on else "NO")

    st.write(report_data_to_update)

    # st.header("DEBUG: REPORT DATA QUERY")
    # st.write(approvals_query)

    # if selected_report and selected_report.mfr_reference_id:
    #     st.write("DEBUG: Row will be added to dataframe.")
