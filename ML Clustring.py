# Databricks notebook source
df = spark.read.csv("/FileStore/tables/data.csv", header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
df = df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# COMMAND ----------

df.show(5)

# COMMAND ----------

row = df.count()
row

# COMMAND ----------

df=df.filter(df.TotalPrice>0)

# COMMAND ----------

df.count()

# COMMAND ----------

df = df.filter(df.CustomerID.isNotNull())
df.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

# COMMAND ----------

df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).show()

# COMMAND ----------

df_filtered=df.groupBy("CustomerID").agg(
    sum("TotalPrice").alias("TotalSpent"),
    sum("Quantity").alias("TotalQuantity")
)
df_filtered.show(5)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
# Assemble the features into a single vector column
feature_cols = ['TotalSpent','TotalQuantity']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
customer_transformed_data = assembler.transform(df_filtered)

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.ml.clustering import KMeans
cost = []
k_values = list(range(2, 11))  # try k from 2 to 10

for k in k_values:
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(customer_transformed_data)
    wssse = model.summary.trainingCost  # within set sum of squared errors
    cost.append(wssse)

# Plotting
plt.plot(k_values, cost, marker='o')
plt.xlabel('Number of clusters (k)')
plt.ylabel('WSSSE (cost)')
plt.title('Elbow Method For Optimal k')
plt.show()

# COMMAND ----------

number_of_clusters = 5
kmeans = KMeans(k = number_of_clusters)
model = kmeans.fit(customer_transformed_data)

# COMMAND ----------

predictions = model.transform(customer_transformed_data)

# COMMAND ----------

predictions.show(20)

# COMMAND ----------

df_cleaned = predictions.drop("features")

# COMMAND ----------

df_cleaned.show(5)

# COMMAND ----------

df_cleaned.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("/tmp/predictions_csv")


# COMMAND ----------

display(dbutils.fs.ls("/tmp/predictions_csv"))

# COMMAND ----------

# Copy the CSV file to FileStore with a simpler name
dbutils.fs.cp(
    "dbfs:/tmp/predictions_csv/part-00000-tid-3096560311503979604-0070b89d-bb9d-4d8e-87d1-c72e7ec56f82-1593-1-c000.csv",
    "dbfs:/FileStore/predictions.csv"
)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))

# COMMAND ----------

# Dossier où se trouve ton CSV généré
csv_folder = "dbfs:/tmp/predictions_csv"

# Lister les fichiers dans ce dossier
files = dbutils.fs.ls(csv_folder)

# Trouver le fichier CSV (part-00000-*.csv)
csv_file = None
for f in files:
    if f.name.startswith("part-") and f.name.endswith(".csv"):
        csv_file = f.path
        break

if csv_file is None:
    raise FileNotFoundError(f"Aucun fichier CSV trouvé dans {csv_folder}")

# Copier vers FileStore
dest_path = "dbfs:/FileStore/predictions.csv"
dbutils.fs.cp(csv_file, dest_path, True)  # overwrite si existant

print("Fichier copié vers :", dest_path)

# Afficher l'URL de téléchargement
workspace_id = "1759547708279745"  # Ton workspace ID

download_url = f"https://community.cloud.databricks.com/?o={workspace_id}#fileStore/predictions.csv"

print("Télécharge ton fichier ici :")
print(download_url)


# COMMAND ----------

# Copier le fichier de DBFS vers le driver local
local_path = "/tmp/predictions.csv"
dbutils.fs.cp("dbfs:/FileStore/predictions.csv", "file:" + local_path, True)

# Lire les premières lignes
with open(local_path, "r") as f:
    for i in range(10):
        print(f.readline())

