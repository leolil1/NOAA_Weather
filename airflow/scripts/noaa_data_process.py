import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

schema = types.StructType([
    types.StructField('STATION', types.StringType(), True), 
    types.StructField('DATE', types.DateType(), True), 
    types.StructField('LATITUDE', types.DoubleType(), True), 
    types.StructField('LONGITUDE', types.DoubleType(), True), 
    types.StructField('ELEVATION', types.DoubleType(), True), 
    types.StructField('NAME', types.StringType(), True), 
    types.StructField('TEMP', types.DoubleType(), True), 
    types.StructField('TEMP_ATTRIBUTES', types.IntegerType(), True), 
    types.StructField('DEWP', types.DoubleType(), True), 
    types.StructField('DEWP_ATTRIBUTES', types.IntegerType(), True), 
    types.StructField('SLP', types.DoubleType(), True), 
    types.StructField('SLP_ATTRIBUTES', types.IntegerType(), True), 
    types.StructField('STP', types.DoubleType(), True), 
    types.StructField('STP_ATTRIBUTES', types.IntegerType(), True), 
    types.StructField('VISIB', types.DoubleType(), True), 
    types.StructField('VISIB_ATTRIBUTES', types.IntegerType(), True), 
    types.StructField('WDSP', types.DoubleType(), True), 
    types.StructField('WDSP_ATTRIBUTES', types.IntegerType(), True), 
    types.StructField('MXSPD', types.DoubleType(), True), 
    types.StructField('GUST', types.DoubleType(), True), 
    types.StructField('MAX', types.DoubleType(), True), 
    types.StructField('MAX_ATTRIBUTES', types.StringType(), True), 
    types.StructField('MIN', types.DoubleType(), True), 
    types.StructField('MIN_ATTRIBUTES', types.StringType(), True), 
    types.StructField('PRCP', types.DoubleType(), True), 
    types.StructField('PRCP_ATTRIBUTES', types.StringType(), True), 
    types.StructField('SNDP', types.DoubleType(), True), 
    types.StructField('FRSHTT', types.LongType(), True)
])

spark = SparkSession.builder \
    .appName('noaa_read_write') \
    .getOrCreate()


df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('/opt/airflow/noaa/19*/*.csv')

df=df.withColumn("Year",df["DATE"].substr(0,4))
years = [df["Year"] for df in df.select("Year").distinct().collect()]

for year in years:
    df.filter(df['Year']==year).drop("Year").write.mode("overwrite").parquet(f'/opt/airflow/noaa/processed/{year}/')

spark.stop()
