# BigData_financial

Welcome to **BigData_financial** — a PySpark-powered project for advanced time series analysis of financial instruments.

## 🚀 Project Overview

This repository demonstrates how to efficiently process and analyze large-scale financial time series data using **PySpark**. The input data is read from `example_input.txt` and passed to a custom-built **calculation module** which performs specific statistical computations based on the instrument type.

## 🎯 Calculations Per Instrument

- **INSTRUMENT1:** Calculates the **mean** value over the entire dataset.
- **INSTRUMENT2:** Calculates the **mean for November 2014** only.
- **INSTRUMENT3:** Applies a dynamic statistical method for real-time outlier detection (or any other online calculation you define).
- **Other instruments:** Computes the **sum of the newest 10 entries** based on date (future enhancement to be announced).

## 🔧 Implementation Highlights

- Used **Spark SQL LEGACY** mode for flexible parsing of dates stored in multiple formats (`DD-MMMM-YYYY` and `DD-MM-YYYY`).
- Defined a custom schema within **SparkSession** to unify and control data types from the start.
- Created the `process_file(file_path)` function to orchestrate all calculations and handle outlier detection with manually set thresholds for clarity. This setup can be easily extended with more advanced techniques.
- Designed a robust logic to fetch final instrument prices via database queries, checking for the presence of a "VALUE" field:
  - If "VALUE" exists, multiply it by 2.
  - Otherwise, fallback to loading raw data from the text file.
- The output includes a new column with the processed values for each instrument and date.

## 📂 Files

- `example_input.txt` — sample input time series data.
- Main processing scripts written in PySpark.

## 🔗 Related Projects

Explore my other work on risk analytics and VaR calculations using PySpark:

[https://github.com/laplasjan/BigData](https://github.com/laplasjan/BigData)
