# Simple ETL Pipeline

## Overview
This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Python. The pipeline extracts data from multiple sources in different formats (CSV, JSON, XML), transforms the data (e.g., rounding prices), and then loads the transformed data into a CSV file. The entire process is logged to a file for transparency and debugging purposes.

## Features
- **Extraction**: Handles CSV, JSON, and XML file formats.
- **Transformation**: Rounds the `price` field to two decimal places.
- **Loading**: Saves the transformed data to a CSV file.
- **Logging**: Logs the ETL process to a text file for monitoring.


## Requirements
- Python 3.x
- pandas library
- xml.etree.ElementTree for XML parsing

You can install the required Python packages using:
```sh
pip install pandas




