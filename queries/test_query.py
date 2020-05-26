import pandas as pd
from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local[*]").appName("Learning_Spark").getOrCreate()

df = pd.read_csv("../youtube-new/CAvideos.csv")
# df = spark.read.csv("../youtube-new/CAvideos.csv", inferSchema=True, header=True, multiLine=True)

# print(df["category_id"])

for i in df:
    print(i)