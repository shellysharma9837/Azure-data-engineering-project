# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
     

# COMMAND ----------

#dbutils.fs.unmount("/mnt/tokyoolympic")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "61123d73-b667-46db-8701-ffaf37a9dc6f",
"fs.azure.account.oauth2.client.secret": 'onI8Q~m~fd3V7j-HjQ_yrn2v0rmOFVMImJVxvco~',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a9518263-24c6-41d6-af92-47517d99aed1/oauth2/token"} 

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdatashell.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)  #Mount creates a shortcut or bridge between Databricks and external storage, so you can easily read and write files without dealing with complex connection code every time.




# COMMAND ----------

#This configs block allows Databricks to authenticate to Azure Storage using OAuth and a Service Principal.

#It’s required when you want to mount Azure Data Lake Storage (Gen2) or Azure Blob Storage securely.

#It replaces using storage account keys or SAS tokens.

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/tokyoolympic/raw data/")

# COMMAND ----------



athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw data/entries.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw data/teams.csv")

# COMMAND ----------

athletes.show() #lazy evaluation
coaches.show()
entriesgender.show()
medals.show()
teams.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Load the athletes data
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw data/athletes.csv")

# 1. Filter out rows where NOC or Discipline is null
athletes_filtered = athletes.filter(col("NOC").isNotNull() & col("Discipline").isNotNull())

# 2. Fill missing NOC with 'Unknown'
athletes_filled = athletes_filtered.fillna({"NOC": "Unknown"})

# 3. Create a new column with uppercase Name
athletes_upper = athletes_filled.withColumn("Name_Upper", upper(col("Name")))

# 4. Create a new column to extract first letter of Discipline
athletes_initials = athletes_upper.withColumn("Discipline_Initial", substring(col("Discipline"), 1, 1))

# 5. Group by NOC and Discipline and count number of athletes
athletes_grouped = athletes_initials.groupBy("NOC", "Discipline").agg(
    F.count("Name").alias("Athlete_Count")
)

# 6. Rank NOCs within each Discipline by number of athletes
window_spec = Window.partitionBy("Discipline").orderBy(desc("Athlete_Count"))
athletes_ranked = athletes_grouped.withColumn("Rank_in_Discipline", row_number().over(window_spec))

#athletes_ranked.show()
# 7. Final DataFrame: Top NOCs per Discipline
athletes = athletes_ranked.filter(col("Rank_in_Discipline") <= 3)

# Show final result
athletes.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window


# 1. Filter out rows where important columns are missing
coaches_filtered = coaches.filter(
    (col("NOC").isNotNull()) & 
    (col("Discipline").isNotNull()) & 
    (col("Event").isNotNull())
)

# 2. Fill missing values (if any left)
coaches_filled = coaches_filtered.fillna({
    "NOC": "Unknown",
    "Discipline": "Unknown",
    "Event": "Unknown"
})

# 3. Create new columns: 
# - Uppercased Name
# - Short Event name (first 5 characters)
# - Discipline Initial
coaches_transformed = coaches_filled \
    .withColumn("Name_Upper", upper(col("Name"))) \
    .withColumn("Event_Short", substring(col("Event"), 1, 5)) \
    .withColumn("Discipline_Initial", substring(col("Discipline"), 1, 1))

# 4. Group by NOC and Discipline and count number of coaches
coaches_grouped = coaches_transformed.groupBy("NOC", "Discipline").agg(
    count("Name").alias("Coach_Count")
)

# 5. Rank NOCs within each Discipline based on number of coaches
window_spec = Window.partitionBy("Discipline").orderBy(desc("Coach_Count"))

coaches_ranked = coaches_grouped.withColumn(
    "Rank_in_Discipline", 
    row_number().over(window_spec)
)

# 6. Select only top 3 NOCs per Discipline
coaches = coaches_ranked.filter(col("Rank_in_Discipline") <= 3)

# Show the final transformed coaches DataFrame
coaches.show()


# COMMAND ----------



# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed data/medals")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed data/teams")