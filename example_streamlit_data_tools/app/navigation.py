import streamlit as st
from time import sleep
from streamlit.runtime.scriptrunner import get_script_run_ctx
from streamlit.source_util import get_pages


def get_current_page_name():
    ctx = get_script_run_ctx()
    if ctx is None:
        raise RuntimeError("Couldn't get script context")

    pages = get_pages("")

    return pages[ctx.page_script_hash]["page_name"]


def make_sidebar():
    with open("./app/theme.css") as f:
        css = f.read()

    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

    with st.sidebar:
        st.title("Data Tools")
        st.write("")
        st.write("")

        if st.session_state.get("logged_in", False):
            user_info = st.session_state.get("user_info")
            # claims = st.session_state.get("claims")
            st.page_link("pages/main.py", label="Main", icon="🔒")
            st.page_link(
                "pages/obfuscated_lookup.py",
                label="Manufacturer Obfuscated ID Lookup",
                icon="🕵️",
            )
            if str(user_info["nickname"]).lower() in ["tim.bytnar"]:
                st.page_link("pages/mfr_reports.py", label="Manufacturer Reporting", icon="🧊")
            if str(user_info["nickname"]).lower() in ["tim.bytnar"]:
                st.page_link("pages/mfr_report_approvals.py", label="Manufacturer Report - Approvals", icon="🧊")
            if str(user_info["nickname"]).lower() in ["tim.bytnar"]:
                st.page_link("pages/mfr_ndcs.py", label="Manufacturer NDCs", icon="💊")
                st.page_link("pages/testing.py", label="Test Playground", icon="💻")
            st.page_link("pages/five9_exports.py", label="Five 9 List Exports", icon="📋")

            st.write("")
            st.write("")

            st.write(f"Logged In As: {user_info['nickname']}")

            if st.button("Log out", type="primary"):
                logout()

        elif get_current_page_name() != "streamlit_app":
            # If anyone tries to access a secret page without being logged in,
            # redirect them to the login page
            st.switch_page("streamlit_app.py")


def logout():
    st.session_state.logged_in = False
    st.info("Logged out successfully!")
    sleep(0.5)
    st.switch_page("streamlit_app.py")
