# Data Engineering Portfolio

## Overview

This repository contains multiple components for data engineering workflows, including Airflow pipelines and a report generation framework. The primary focus is on data ingestion, transformation, loading, and reporting using tools like Airflow, Snowflake, and AWS S3.

---

## Airflow Data Pipeline

### Overview

The Airflow Data Pipeline is designed to facilitate data ingestion, transformation, and loading processes. It handles various data sources, performs data quality checks, and loads the processed data into Snowflake for further analysis.

### Features

- **Source-to-Raw Layer**: Extracts data from multiple sources (e.g., SFTP, S3) and stores it in the raw layer.
- **Raw-to-Staging Layer**: Transforms raw data, performs data quality checks, and uploads it to the staging layer in S3.
- **Staging-to-Snowflake**: Loads the staged data into Snowflake for analytics and reporting.
- **Dynamic DAG Creation**: DAGs are dynamically generated for each dataset based on configuration.
- **Data Quality Checks**: Ensures data integrity before loading into the staging or Snowflake layers.

### Repository Structure

```
/airflow_data_pipeline/
├── dags/
│   ├── Pipeline/
│   │   ├── asset_based_pipeline.py  # Main pipeline logic for DAG creation
│   │   └── ...                      # Additional pipeline files
│   └── ...                          # Other DAGs
├── common_library/
│   ├── common_pipeline.py           # Core pipeline functions (e.g., RawToStage, StageToSnowflake)
│   └── ...                          # Other shared utilities
└── README.md                        # Project documentation
```

### How It Works

1. **Dataset Configuration**: Each dataset is defined with its provider, connection type, target schema, and table. The configuration determines the pipeline's behavior.
2. **Dynamic DAG Creation**: DAGs are generated dynamically for each dataset using the `create_dag` function.
3. **Task Execution**:
   - `source_to_raw`: Extracts data from the source and stores it in the raw layer.
   - `raw_to_stage`: Transforms raw data and uploads it to the staging layer.
   - `stage_to_snowflake`: Loads staged data into Snowflake.
4. **Branching Logic**: Conditional tasks determine whether to proceed with downstream tasks based on the presence of files.

---

## Airflow Report Generator

### Overview

The `airflow_report_generator` is a framework for generating dynamic reports using Airflow. It allows users to define SQL queries, table schemas, and report configurations to automate the creation of data-driven reports.

### Features

- **Dynamic SQL Execution**: Executes user-defined SQL queries to generate report data.
- **Customizable Configurations**: Supports YAML-based configuration for report parameters.
- **Snowflake Integration**: Queries and stores report data in Snowflake.
- **Reusable Components**: Includes utilities for managing SQL queries and table schemas.

### Repository Structure

```
/airflow_report_generator/
├── dags/
│   ├── Reporting/
│   │   ├── universal_report_generator.py  # Main DAG for report generation
│   │   └── Reports/
│   │       └── Recipient/
│   │           └── Example_Report/
│   │               ├── query.sql          # SQL query for the report
│   │               ├── report_config.yaml # Configuration for the report
│   │               └── table_ddl.sql      # Table schema for the report
├── requirements/
│   └── requirements.txt                   # Python dependencies
└── README.md                              # Project documentation
```

### How It Works

1. **Report Configuration**: Define the report parameters in a YAML file (`report_config.yaml`), including:
   - Report name
   - Target table
   - Date range
   - Other custom parameters
2. **SQL Query**: Write the SQL logic for the report in `query.sql`. Use placeholders (e.g., `%DATESTART%`, `%DATEEND%`) for dynamic date ranges.
3. **Table Schema**: Define the schema for the report table in `table_ddl.sql`.
4. **DAG Execution**: The `universal_report_generator.py` DAG reads the configuration, executes the SQL query, and writes the results to the target table in Snowflake.

### Example Report Workflow

1. **Query Execution**: Executes the SQL query defined in `query.sql` with the specified parameters.
2. **Table Creation**: Creates the target table in Snowflake using the schema defined in `table_ddl.sql`.
3. **Data Insertion**: Inserts the query results into the target table.
4. **Report Completion**: Marks the report as complete and logs the execution details.

---

## Prerequisites

- **Airflow**: Ensure Airflow is installed and configured.
- **Snowflake**: A Snowflake account and credentials are required for data loading and reporting.
- **AWS S3**: Used for storing raw and staged data.

---

## Usage

### Airflow Data Pipeline

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd airflow_data_pipeline
   ```

2. Configure datasets in the appropriate files under `Pipeline/Datasets`.

3. Start the Airflow scheduler and webserver:
   ```bash
   airflow scheduler &
   airflow webserver
   ```

4. Monitor DAGs in the Airflow UI.

### Airflow Report Generator

1. Navigate to the `airflow_report_generator` directory:
   ```bash
   cd airflow_report_generator
   ```

2. Define your report configuration in `report_config.yaml`.

3. Write the SQL query in `query.sql` and the table schema in `table_ddl.sql`.

4. Trigger the `universal_report_generator.py` DAG in the Airflow UI.

---

## Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a feature branch.
3. Submit a pull request with a detailed description of your changes.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---

## Contact

For questions or support, please contact the repository maintainer.