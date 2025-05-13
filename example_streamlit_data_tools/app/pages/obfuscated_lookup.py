# streamlit_app.py

from navigation import make_sidebar
import streamlit as st
from st_aggrid import JsCode, GridOptionsBuilder, AgGrid, GridUpdateMode

st.set_page_config(
    page_title="Obfuscated ID Lookup",
    page_icon="🧊",
    layout="centered",
    initial_sidebar_state="expanded",
)

make_sidebar()
# Initialize connection.
conn = st.connection("snowflake")

# Perform query.
df = conn.query(
    "SELECT RECIPIENT_ID::STRING as RECIPIENT_ID, ID_TYPE::STRING as ID_TYPE, OBFUSCATED_ID::STRING as OBFUSCATED_ID, ORIGINAL_ID::STRING as ORIGINAL_ID FROM PROD_REPORTS_CORE.METADATA.OBFUSCATED_IDS;",
    ttl=600,
)

st.write("### Lookup Obfuscated CCIDs")

obsid_search = st.text_input("Search for Obfuscated ID", value="").split(",")
smc_search = st.text_input("Search for SMC ID", value="").split(",")
mfr_option = st.selectbox("Manufacturer", ("example1", "example2", "Any"), index=0)

smc_df = df["ORIGINAL_ID"].isin(smc_search)
obs_df = df["OBFUSCATED_ID"].isin(obsid_search)

# https://sm-foundation.inovalonone.com/patient/detail/439959


def gen_url(row):
    if row["ID_TYPE"] == "patient_id":
        return f"https://sm-foundation.inovalonone.com/patient/detail/{str(row['ORIGINAL_ID'])}"
    else:
        return None


df["URL"] = df.apply(gen_url, axis=1)
results_df = df
if mfr_option != "Any":
    mfr_df = df["RECIPIENT_ID"].astype(str).str.match(mfr_option.upper())
    results_df = results_df[mfr_df]

if smc_search[0] != "":
    results_df = results_df[smc_df]

if obsid_search[0] != "":
    results_df = results_df[obs_df]

gb = GridOptionsBuilder.from_dataframe(results_df)
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
go = gb.build()
AgGrid(results_df, gridOptions=go, allow_unsafe_jscode=True, height=500)


# DEBUG SECTION
user_info = st.session_state.get("user_info")
if str(user_info["nickname"]).lower() in ["tim.bytnar"]:
    debug = True
else:
    debug = False

if debug:
    st.header("DEBUG SECTION")
    st.write(f"Obfuscated IDs: {obsid_search}")
    st.write(f"SMC IDs: {smc_search}")
    st.write(f"Manufacturer: {mfr_option}")
    st.write(f"Results: {results_df}")
    st.write(f"MFR DF: {mfr_df}")
