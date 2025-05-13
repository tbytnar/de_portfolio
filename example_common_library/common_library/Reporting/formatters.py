import csv
import pandas as pd
from datetime import datetime
from common_library.common_reports import ReportDestination
import common_library.common_config as config
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


WORKING_PATH = config.working_directory


def format_csv(df: pd.DataFrame, filename, destination: ReportDestination):
    include_record_count = False if "include_record_count" not in destination.formatter.args else destination.formatter.args["include_record_count"]
    include_header = True if "include_header" not in destination.formatter.args else destination.formatter.args["include_header"]
    row_seperator = "\r\n" if "row_seperator" not in destination.formatter.args else destination.formatter.args["row_seperator"]
    column_seperator = "," if "column_seperator" not in destination.formatter.args else destination.formatter.args["column_seperator"]

    if include_record_count:
        output_template = f"""Records|{len(df)}\n""" + "{}"
    else:
        output_template = "{}"

    df = df.replace({float("nan"): None})

    df.columns = df.columns.str.replace('"', "")

    logger.info(f"Row_Seperator: {row_seperator}")

    logger.info(df.head(3))

    with open(os.path.join(WORKING_PATH, filename), "w") as f:
        f.write(output_template.format(df.to_csv(sep=column_seperator, header=include_header, lineterminator=row_seperator, index=False)))
    if os.path.isfile(os.path.join(WORKING_PATH, filename)):
        return True
    else:
        return False


def format_excel(df: pd.DataFrame, filename, destination: ReportDestination):
    logger.info(df)
    logger.info("FORMATTING AS EXCEL")
    df = df.replace({float("nan"): None})
    df.to_excel(os.path.join(WORKING_PATH, filename), sheet_name="sheet1", index=False)
    if os.path.isfile(os.path.join(WORKING_PATH, filename)):
        return True
    else:
        return False


def format_bcbst_copay(df: pd.DataFrame, filename, destination: ReportDestination):
    data_row = df["DATAROW"]
    df = df.replace({float("nan"): None})
    row_seperator = "\r\n" if "row_seperator" not in destination.formatter.args else destination.formatter.args["row_seperator"]
    column_seperator = "," if "column_seperator" not in destination.formatter.args else destination.formatter.args["column_seperator"]

    header_string = f"H{datetime.now().strftime('%Y%m%d')}22"
    total_records = len(df)
    total_paid = df["GROSSAMOUNTPAID"].astype(float).sum()
    footer_string = "F" + "{:0>9}".format(total_records) + "{:0=+15.2f}".format(float(total_paid))

    with open(os.path.join(WORKING_PATH, filename), "w") as f:
        f.write(f"{header_string}")
        writer = csv.writer(f, delimiter=column_seperator, lineterminator=row_seperator)
        writer.writerow(data_row)
        f.write(f"{footer_string}\n")

    if os.path.isfile(os.path.join(WORKING_PATH, filename)):
        logger.info(f"{filename} created successfully!")
        return True
    else:
        logger.error(f"{filename} failed to create!")
        return False
