# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
    mount_point = "/mnt/iotdata", #mount location
    extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>",key = "<key-name>")}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/tokyoolympicdata/raw-data/")

# COMMAND ----------

athletes = spark.read.format("csv").options(header = 'True', inferSchema = 'True').load('dbfs:/mnt/tokyoolympicdata/raw-data/athletes.csv')
coaches = spark.read.format("csv").options(header = 'True', inferSchema = 'True').load('dbfs:/mnt/tokyoolympicdata/raw-data/coaches.csv')
entriesgender = spark.read.format("csv").options(header = 'True', inferSchema = 'True').load('dbfs:/mnt/tokyoolympicdata/raw-data/entriesgender.csv')
medals = spark.read.format("csv").options(header = 'True', inferSchema = 'True').load('dbfs:/mnt/tokyoolympicdata/raw-data/medals.csv')
teams = spark.read.format("csv").options(header = 'True', inferSchema = 'True').load('dbfs:/mnt/tokyoolympicdata/raw-data/teams.csv')

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

#find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold",ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

#calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female',entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male',entriesgender['Male'] / entriesgender['Total']
).select("Discipline","Avg_Female","Avg_Male")
average_entries_by_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").options(header = 'True').csv("mnt/tokyoolympicdata/transformed-data/athletes")

# COMMAND ----------

coaches.repartition(1).write.mode("overwrite").options(header = 'True').csv("mnt/tokyoolympicdata/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").options(header = 'True').csv("mnt/tokyoolympicdata/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").options(header = 'True').csv("mnt/tokyoolympicdata/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").options(header = 'True').csv("mnt/tokyoolympicdata/transformed-data/teams")
