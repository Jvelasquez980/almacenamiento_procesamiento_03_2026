import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = args['BUCKET']  # ej: mi-bucket-medallion-lab
BRONZE = f's3://{BUCKET}/bronze/ventas/'
SILVER = f's3://{BUCKET}/silver/ventas/'

# Leer Bronze
df_b = spark.read.option('multiline', 'true').json(BRONZE)

# Transformaciones Silver (iguales al notebook local)
df_silver = df_b \
    .dropDuplicates(['order_id']) \
    .filter(F.col('order_id').isNotNull()) \
    .withColumn('order_date',   F.to_date('order_date', 'yyyy-MM-dd')) \
    .withColumn('order_year',   F.year('order_date')) \
    .withColumn('order_month',  F.month('order_date')) \
    .withColumn('unit_price',   F.col('unit_price').cast(DoubleType())) \
    .withColumn('quantity',     F.col('quantity').cast(IntegerType())) \
    .withColumn('discount',     F.col('discount').cast(DoubleType())) \
    .withColumn('gross_amount', F.round(F.col('quantity') * F.col('unit_price'), 2)) \
    .withColumn('net_amount',   F.round(
        F.col('quantity') * F.col('unit_price') * (1 - F.col('discount')), 2)) \
    .withColumn('status',       F.lower(F.trim('status')))

# Escribir Silver en S3
df_silver.write \
    .mode('overwrite') \
    .partitionBy('order_year', 'order_month') \
    .parquet(SILVER)

job.commit()