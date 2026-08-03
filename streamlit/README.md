# IoT Analytics Streamlit Dashboard

## Data source

The application queries:

`HACKATHON_IOT.ANALYTICS.GOLD_DEVICE_DAILY`

It does not query the RAW or CLEAN layer directly.

## Required visualizations

- Device activity map
- AQI time-series trend
- Top-N devices chart
- Automatic refresh every 30 seconds

## Authentication

The local launcher provides Snowflake key-pair values through process-level
environment variables. Credentials and passphrases are not stored in the
application source code.

## Start

Run the project PowerShell setup/launcher and open:

`http://localhost:8501`