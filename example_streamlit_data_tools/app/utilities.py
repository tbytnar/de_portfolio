import streamlit as st
import streamlit.components.v1 as components
import json


def log_to_console(message: str) -> None:
    js_code = f"""
<script>
    console.log({json.dumps(message)});
</script>
"""
    components.html(js_code)


def PopulateContainer(elements, category):
    if elements["columns"][0]["name"] != "REPLACEME":
        st.header(category, divider=True)
        col1, col2, col3 = st.columns(3)
        n = len(elements["columns"])
        part1_end = n // 3
        part2_end = 2 * n // 3

        first_part = elements["columns"][:part1_end]
        second_part = elements["columns"][part1_end:part2_end]
        third_part = elements["columns"][part2_end:]

        for column in first_part:
            column["value"] = col1.text_input(label=column["name"], on_change=None)

        for column in second_part:
            column["value"] = col2.text_input(label=column["name"], on_change=None)

        for column in third_part:
            column["value"] = col3.text_input(label=column["name"], on_change=None)


@st.cache_data
def format_df(df, filename, include_record_count, include_header, column_seperator):
    df = df.reset_index(drop=True)
    with open(filename, "w") as f:
        if include_record_count:
            f.write(f"Records|{len(df)}\n")
        if include_header:
            columns_string = column_seperator.join(x for x in list(df.columns))
            columns_string = columns_string + "\n"
            columns_string = columns_string.replace('"', "")
            f.write(f"{columns_string}")
        df.to_csv(f, sep=column_seperator, index=False, header=False)

    with open(filename, "r") as f:
        return f.read().encode("utf-8")
