import streamlit as st
import pandas as pd
from navigation import make_sidebar

st.set_page_config(
    page_title="Manufacturer NDCs",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

make_sidebar()

conn = st.connection("snowflake")

mfr_query = "SELECT MANUFACTURER, NDC_NUMBER::STRING as NDC_NUMBER FROM PROD_FIVE9_STAGE.METADATA.MFR_NDC_MAPPING"

if "mfr_df" not in st.session_state:
    st.session_state.mfr_df = conn.query(mfr_query, ttl=600)


col1, col2 = st.columns([2, 1], gap="medium")

with col1:
    edited_df = st.data_editor(st.session_state.mfr_df, use_container_width=True, hide_index=True)
with col2:
    Add = st.button("Add")
    Remove = st.button("Remove")
    Reset = st.button("Reload Data")
    Save = st.button("Save")

if Add:
    new_df = pd.DataFrame([{"MANUFACTURER": None, "NDC_NUMBER": None}])

    # Add this new_df to the existing st.session_state.df.
    st.session_state.mfr_df = pd.concat([st.session_state.mfr_df, new_df], ignore_index=True)
    st.rerun()  # to rerender the updated value of st.session_state.df

if Remove:
    st.session_state.mfr_df = st.session_state.mfr_df.drop(st.session_state.mfr_df.index[-1])
    st.rerun()

if Reset:
    st.session_state.mfr_df = conn.query(mfr_query, ttl=600)
    st.rerun()

if Save:
    # conn.query("DELETE FROM PROD_FIVE9_STAGE.METADATA.MFR_NDC_MAPPING_TEST;")
    cur = conn.cursor()
    cur.execute("DELETE FROM PROD_FIVE9_STAGE.METADATA.MFR_NDC_MAPPING_TEST;")
    conn.write_pandas(
        edited_df,
        table_name="MFR_NDC_MAPPING_TEST",
        database="PROD_FIVE9_STAGE",
        schema="METADATA",
    )
    st.toast("Saved to Database")
