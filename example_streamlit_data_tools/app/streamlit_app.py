from auth0_component import login_button
import streamlit as st
from navigation import make_sidebar
from time import sleep
import requests
import json
import http.client

make_sidebar()

st.title("Welcome to the Data Tools Application")

auth0_api_url = "AUTH0_API_URL"  # Replace with your Auth0 API URL

clientId = st.secrets["Auth0"]["CLIENT_ID"]
clientsecret = st.secrets["Auth0"]["CLIENT_SECRET"]
api_client_id = st.secrets["Auth0"]["API_CLIENT_ID"]
api_client_secret = st.secrets["Auth0"]["API_CLIENT_SECRET"]
domain = st.secrets["Auth0"]["DOMAIN"]

if "auth0_api_token" not in st.session_state:
    conn = http.client.HTTPSConnection("example.us.auth0.com")

    payload = json.dumps(
        {
            "client_id": api_client_id,
            "client_secret": api_client_secret,
            "audience": "https://example.us.auth0.com/api/v2/",
            "grant_type": "client_credentials",
        }
    )
    headers = {"content-type": "application/json"}
    conn.request("POST", "/oauth/token", payload, headers)

    res = conn.getresponse()
    data = res.read()

    data_json = json.loads(data.decode("utf-8"))

    st.session_state["auth0_api_token"] = data_json.get("access_token")

if "logged_in" not in st.session_state:
    user_info = login_button(clientId=clientId, domain=domain)
    if user_info:
        st.session_state.logged_in = True
        st.success("Logged in successfully!")
        sleep(0.5)

        auth_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {st.session_state["auth0_api_token"]}",
        }

        response = requests.request("GET", auth0_api_url, headers=auth_headers, data={})
        st.write(response.text)
        role_user_list = [x["user_id"] for x in json.loads(response.text)]

        if user_info["sub"] in role_user_list:
            st.session_state["user_info"] = user_info
            st.switch_page("pages/main.py")
        else:
            st.error("You do not have the correct role to access this application.")

    if not user_info:
        st.write("Please login to continue")
