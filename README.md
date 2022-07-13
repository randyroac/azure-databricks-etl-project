# azure-databricks-etl-project

## ETL motor racing data
####The data is from the Ergast website. 

The data is stored in the form of an API, downloadable CSVs, and nested or non-nested JSON files.
Azure Databricks on top of Apache Spark, Azure Notebook, and  Azure Data Lakes Storage are the main tools for this ETL Project.

In this project, I focused on extraction from the CSV AND JSON files for my ETL. This can be done on a free AZURE trial option from Microsoft.

Here is a quick diagram of the high-level plan.

![etl_motor_racing_1](https://user-images.githubusercontent.com/89104037/178783258-7ddec9c9-c09f-4464-830b-50f3effb2623.jpg)

### Quick Overview of my ETL Processes

Purple Blocks show columns were renamed and/or transformed
Red Blocks show columns that were dropped
Green Blocks show columns that were Added

![etl_motor_racing_2](https://user-images.githubusercontent.com/89104037/178783267-33977204-97d2-418d-ac0d-594711ee3174.jpg)

![etl_motor_racing_3](https://user-images.githubusercontent.com/89104037/178783276-34bbe7e5-694c-4402-b2ab-dea585c7a976.jpg)

Both horizontal and vertical scaling is very much possible but a larger budget would be necessary to truly take advantage of the full potential of Azure Databricks.

![etl_motor_racing_4](https://user-images.githubusercontent.com/89104037/178783282-19667f35-7021-4a76-a139-9eab6e32c07a.jpg)


Below are random snapshots the reproducable files are avalable DataBricks files are in the folder


### Creating secure secret keys and connecting  and create and mounting the raw empty folder

![etl_adls_notebook_1](https://user-images.githubusercontent.com/89104037/178783429-b7738916-8142-4f13-ab6a-79533689590c.png)

### Uploading raw files to Data Lakes Storage raw folder

![etl_adls_notebook_2](https://user-images.githubusercontent.com/89104037/178783436-6413f66c-09cc-4b0d-9a63-e304d566c2dd.png)

### read the json file using the spark dataframe

![etl_adls_notebook_3](https://user-images.githubusercontent.com/89104037/178783444-ab6adc7d-72f2-415c-a1a9-e4d5af9f17c0.png)

### Output to parquet file

![etl_adls_notebook_4](https://user-images.githubusercontent.com/89104037/178783446-f04e5ef8-ca01-4942-b260-4299a969af55.png)
