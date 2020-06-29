from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
import ast

runner, conf = sys.argv
conf = ast.literal_eval(conf)

spark = SparkSession.builder \
    .appName('Fourcast GPS Data') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', '%TEMP_BUCKET%')

import_schema = StructType(
    [
        StructField('taxi_id', StringType()),
        StructField('timestamp', IntegerType()),
        StructField('latitude', DoubleType()),
        StructField('longitude', DoubleType())
    ]
)

export_schema = StructType(
    [
        StructField('taxi_id', StringType()),
        StructField('timestamp', TimestampType()),
        StructField('latitude', DoubleType()),
        StructField('longitude', DoubleType()),
        StructField('location', StringType()),
        StructField('date', DateType())
    ]
)
df = spark.read.csv('gs://{}/{}'.format(conf['bucket'], conf['name']), header=True, schema=import_schema)

mid = df.dropna() \
    .distinct() \
    .withColumn('location', F.format_string("POINT(%s %s)",
                                            F.col('latitude'),
                                            F.col('longitude'))) \
    .withColumn('date', F.from_unixtime(F.col('timestamp'), 'yyyy-MM-dd').cast(DateType())) \
    .withColumn('timestamp', F.from_unixtime(F.col('timestamp')).cast(TimestampType())) \
    .select(['taxi_id', 'timestamp', 'latitude', 'longitude', 'location', 'date'])

output = SQLContext(spark).createDataFrame(mid.rdd, export_schema)

output.write \
    .format('bigquery') \
    .option('table', '%BQ_DATASET%.%GPS_TABLE%') \
    .option("encoding", "UTF-8") \
    .mode('append') \
    .save()