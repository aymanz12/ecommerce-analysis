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

