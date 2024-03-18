# Snowflake ETL Project: End-to-End Data Pipeline for Customer Sales

### Project Highlights

- **Multi-Schema Architecture**: Implemented a robust architecture utilizing three distinct schemas within **Snowflake**
  - **landing_zone** for raw data ingestion
  - **curated_zone** for data cleaning and transformation
  - **consumption_zone** for business-level aggregations.
- **AWS S3 Integration**: Integrated AWS S3 for continuous data loading, ensuring seamless ingestion of batch data and enabling real-time analytics.
- **Comprehensive Data Processing**: Developed comprehensive ETL workflows covering customer, orders, and products data, ensuring the integrity and accuracy of the entire data pipeline.
- **Change Data Capture**: Implemented Change Data Capture (CDC) mechanisms, including pipes, streams, and tasks, for handling Slowly Changing Dimension (SCD) Type 2 data, allowing for historical tracking and analysis of changes over time.
- **Data Quality and Consistency**: Implemented data quality checks and validations at each stage of the pipeline to ensure consistency and reliability of the data for downstream analytics and reporting.

### Architecture

![snow_arch](https://github.com/raj0512gaurav/cusomer_orders_etl/assets/56684761/08fe8097-8378-4c31-9941-99271096cb20)
