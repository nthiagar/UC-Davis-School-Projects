# UC Davis School Projects

## Overview
This repository contains various projects I completed as part of my coursework at UC Davis.

### 1. STA 108
- **File:** `STA 108/STA 108 Project 1.Rmd`
- **Description:** This project involves analyzing the CDI data in collaboration with Leticia Effrien and Victoria Tomlinson.
  - Fitting regression models
  - Measuring linear associations
  - Performing inference about regression parameters
  - Conducting regression diagnostics
  - Discussing the impact of observational data on the results

- **File:** `STA 108/STA 108 Project 2.Rmd`
- **Description:** Building on Project 1, this project extends the analysis of the CDI data with multiple linear regression techniques. Collaborated with Leticia Effrien and Victoria Tomlinson.
  - Fitting models with two-factor interactions
  - Computing partial determination coefficients
  - Discussing results from a practical standpoint
  - Expanding on the analysis conducted in Project 1

- **Data Source:** `STA 108/CDI.txt`

### 2. STA 137
- **File:** `STA 137/STA 137 Final Project.pdf`
- **Description:** This project focuses on analyzing the Central African Republic Exports time series data from 1960 to 2017 using ARIMA models. Collaborated with Adithi Sumitran and Bradley Parmer-Lohan.
  - Describing the dataset
  - Conducting a detailed step-by-step analysis using class-taught methods
  - Fitting an appropriate ARIMA model
  - Presenting overall conclusions with a focus on clear communication to a non-expert audience

### 3. STA 141A
- **File:** `STA 141A/STA 141A Final Project.Rmd`
- **Description:** This project evaluates the relationship between types and rates of crime on public university campuses in California and various economic, academic, and external factors. Collaborated with Karla Cornejo Argueta, Matthew Holcomb, and Xiaoxiao Huang.
  - Identifying correlations between county crime rates and economic factors
  - Exploring economic factors using university tuition rates as an indicator of campus-specific economic conditions
  - Using logistic regression to predict UC or CSU classification based on different pairs of factors
  - Constructing various models to explore other potential relationships between campus crime and external factors

- **Data Source:** `STA 141A/ProjectData.xlsx`

### 4. ECS 116
- **Folder:** `ECS 116/Postgres Project`
- **Description:** This project focused on uploading and analyzing large-scale Airbnb NYC data in PostgreSQL to study SQL query performance. Collaborated with Cindy Chen and Connor Young.
  - Uploaded 15M+ records using DBeaver and SQL COPY commands
  - Created a Python test harness to benchmark joins, text search, and updates
  - Generated JSON logs and visualized results using matplotlib
  - Analyzed the impact of indexing on query and update performance

- **Folder:** `ECS 116/MongoDB Project`
- **Description:** Created embedded MongoDB collections for Airbnb listings by aggregating calendar and review data using pipelines. Collaborated with Cindy Chen and Connor Young.
  - Built nested documents using aggregation pipelines and lookup, group, and unwind stages
  - Combined reviews and availability data into a unified collection
  - Created JSON exports and performed performance comparisons with and without indexing
  - Benchmarked text search queries using MongoDB’s text index for “good” and “bad” listings

### 5. ECS 119  
- **Folder:** `ECS 119/Project 1`  
- **Description:** This project involved analyzing QS World University Rankings data from 2019–2021 using Pandas to practice data processing and statistical analysis.
  - **Part 1:** Cleaned and validated datasets, performed sampling, explored data quality, calculated regional averages, and built visualizations (box plots, scatter plots, correlation matrices) to examine ranking trends  
  - Created a custom ranking system to simulate data manipulation and observed how falsified metrics can bias university rankings  
  - **Part 2:** Designed and benchmarked data pipelines using throughput and latency metrics, comparing:
    - File-based input vs. in-memory DataFrames  
    - Vectorized Pandas operations vs. traditional for-loops  
    - Visualized performance using matplotlib and formed hypotheses on pipeline efficiency  
  - **Part 3:** Built shell-based scripts to automate setup and file processing; compared shell vs. Python-based workflows for row counting using performance metrics
 
- **Folder:** `ECS 119/Project 2`  
- **Description:**  Built, analyzed, and benchmarked custom MapReduce pipelines using PySpark, with a focus on understanding scalability, performance, and dataflow abstraction.
  - **Part 1:** Implemented generalized map and reduce functions for flexible MapReduce pipelines. Applied them to analyze patterns in large numeric datasets (e.g., digit and letter frequencies from 1 to 1 million). Investigated edge cases like empty outputs and nondeterministic results due to parallel execution.
 - **Part 2:** Created a detailed dataflow graph representing how data moves through the pipeline, highlighting transformations and shared computations across questions.
 - **Part 3:** Evaluated real-world performance using configurable input sizes and parallelism levels. Measured throughput and latency across multiple configurations (1–16 partitions, up to 1M inputs) and visualized results. Reflected on differences between theoretical models and actual performance due to overheads and system-level factors.
