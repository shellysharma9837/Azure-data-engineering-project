Tokyo Olympics 2021 - Azure Data Engineering Project

📚 Project Overview

This project demonstrates an end-to-end cloud data pipeline built on Microsoft Azure, showcasing data ingestion, transformation, storage, and analysis workflows using various Azure services.

The dataset used is based on the Tokyo Olympics 2021 events and medal winners.

🛠️ Tools & Technologies Used
GitHub (Source Data Repository)

Azure Storage Account (Data Storage)

Azure Data Factory (Data Copy/Ingestion)

Azure Databricks (Data Transformation using PySpark)

Azure Synapse Analytics (External Table Creation and Data Analysis)

PySpark (Data Processing and Cleaning)

🔥 Project Workflow
Data Ingestion

Uploaded the Tokyo Olympics 2021 dataset to GitHub.

Created a Linked Service in Azure to connect GitHub to Azure Storage Account.

Copied the raw dataset from GitHub to Azure Blob Storage using Azure Data Factory.

Data Transformation

Loaded the raw data from Azure Storage into Azure Databricks.

Performed data cleaning, formatting, and transformation using PySpark.

Data Loading

Saved the transformed data back to Azure Blob Storage.

Data Analysis

Created external tables in Azure Synapse Analytics by connecting to the transformed data.

Ran analytical queries to gain insights from the Olympics dataset (e.g., country-wise medals, event participation statistics).

🎯 Key Learnings
Building scalable data pipelines on Azure.

Hands-on experience with Linked Services, Datasets, and Dataflows.

Data Transformation using PySpark in Azure Databricks.

Creating and querying external tables in Azure Synapse.

End-to-end integration of Azure services for a real-world dataset.

📈 Future Enhancements
Implement incremental data loading.

Build a Power BI dashboard for interactive visualizations.

Introduce data validation pipelines for better quality control.
