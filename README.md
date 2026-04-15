# KPT-Zambia-Public
Data analysis of Kitchen Performance Test (KPT) sensor data: An intervention study on fuel efficiency in Zambia.

This project contains:
- ETL Pipeline
- SQLite Database

and

## Dummy Data
This directory contains sample data files that mimic the format of the original sensor data.
These files are provided so users can run the code without accessing the sensitive original data.

## File Naming Convention
The dummy files follow the same naming pattern as the original data:
- EXACT sensor data: `EXACTv2_dummy_[SENSOR_ID]_[START_TIMESTAMP]_CLEAN.csv`
- FUEL sensor data: `FUELv2_dummy_[SENSOR_ID]_[START_TIMESTAMP]_CLEAN.csv`
Each file contains a reduced number of data points (typically 10 rows) but maintains the same column structure and metadata format as the original files.

Webapp is coming soon! But for now:

## Usage
To use this dummy data with the analysis scripts:
1. Copy the desired dummy files to the appropriate directories (ECS_EXACT/, ECS_FUEL/, etc.)
2. Remove the "_dummy" prefix from the filename to match the expected format
3. Run your analysis scripts as normal
Note: The original data directories (ECS_EXACT/, ECS_FUEL/, ECS_RAW/) are excluded from this repository via .gitignore due to data sensitivity.
