from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import sqlite3
import pandas as pd

spark = SparkSession.builder \
    .appName("Time Series Data Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

schema = StructType([
    StructField("INSTRUMENT", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("VALUE", DoubleType(), True)
])

def process_file(file_path):
    df = spark.read.csv(
        path=file_path,
        schema=schema,
        header=False,
        sep=","
    )

    df = df.withColumn("DATE", F.to_date("DATE", "dd-MMM-yyyy"))

    df_instrument2 = (
        df.filter(F.col("INSTRUMENT") == "INSTRUMENT2")
        .filter((F.col("DATE") >= F.lit(datetime(2014, 11, 1))) & (F.col("DATE") < F.lit(datetime(2014, 12, 1))))
        .withColumn("count", F.lit(None).cast("long"))
    )

    df_instrument1 = df.filter(F.col("INSTRUMENT") == "INSTRUMENT1")
    window_avg = Window.partitionBy("INSTRUMENT").orderBy("DATE").rowsBetween(-2, 0)
    window_lag = Window.partitionBy("INSTRUMENT").orderBy("DATE")

    df_instrument1 = (
        df_instrument1.withColumn("avg_value", F.avg("VALUE").over(window_avg))
        .withColumn("VALUE", F.lag("avg_value").over(window_lag))
        .drop("avg_value")
        .withColumn("count", F.lit(None).cast("long"))
    )

    df_instrument3 = df.filter(F.col("INSTRUMENT") == "INSTRUMENT3")
    df_instrument3 = (
        df_instrument3.withColumn(
            "count",
            F.sum(F.when(F.col("VALUE") > 100, 1).otherwise(0)).over(
                Window.partitionBy("INSTRUMENT").orderBy("DATE").rowsBetween(Window.unboundedPreceding, 0)
            )
        )
        .withColumn("VALUE", F.col("VALUE").cast("double"))
    )

    result_df = df_instrument1.unionByName(df_instrument2).unionByName(df_instrument3)
    result_df = result_df.orderBy("INSTRUMENT", "DATE")
    return result_df

input_path = "C:/Users/jmich/.vscode/task1/example_input.txt"

conn = sqlite3.connect("INSTRUMENT_PRICE_MODIFIER.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS INSTRUMENT_PRICE_MODIFIER (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL
)
""")

chunksize = 100000
for chunk in pd.read_csv(input_path, header=None, names=["instrument", "date", "value"], chunksize=chunksize):
    chunk['instrument'] = chunk['instrument'].astype(str)
    chunk['date'] = chunk['date'].astype(str)
    chunk['value'] = pd.to_numeric(chunk['value'], errors='coerce')
    chunk.to_sql('INSTRUMENT_PRICE_MODIFIER', conn, if_exists='append', index=False)

cursor.execute("PRAGMA table_info(INSTRUMENT_PRICE_MODIFIER)")
columns = [col[1] for col in cursor.fetchall()]
if "DOUBLE_VALUE" not in columns:
    cursor.execute("ALTER TABLE INSTRUMENT_PRICE_MODIFIER ADD COLUMN DOUBLE_VALUE REAL")

cursor.execute("""
    UPDATE INSTRUMENT_PRICE_MODIFIER
    SET DOUBLE_VALUE = value * 2
    WHERE value IS NOT NULL
""")

conn.commit()
conn.close()

result_df = process_file(input_path)
result_df.show()
