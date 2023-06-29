import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
schema = StructType([
        StructField("client_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("humidity", IntegerType()),
        StructField("pitch", StringType()),
        StructField("roll", StringType()),
        StructField("yaw", StringType()),
        StructField("count", IntegerType()),
        StructField("nested", StructType([
            StructField("temperature", IntegerType()),
            StructField("pressure_test", IntegerType())
        ]))
    ])
    
def process_batch(data_frame, batch_id):
    dfc = data_frame.cache()
    logger.info(" - my_log - process batch id: " + str(batch_id) + " record number: " + str(dfc.count()))    
    
    data_frame.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://<redshift_host_name>m:5439/dev?user=<redshift_user>&password=<redshift_password>") \
    .option("dbtable", "iot_nested_json_sample_table") \
    .option("tempdir", "s3://innovator-island-bucket-ericacn/spark-redshift-cdc/tmpdir/") \
    .option("aws_iam_role", "arn:aws:iam::<your_account_id>:role/service-role/AmazonRedshift-CommandsAccessRole-20230523T112923") \
    .mode("error") \
    .save()
  
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "<your kafka broker>") \
  .option("subscribe", "kafka-spark-streaming-test_nested") \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", False) \
  .load() \
  .selectExpr("CAST(value AS string)")


value_df = df.select(from_json(col("value").cast("string"),schema).alias("parse_value"))

final_df = value_df.select('parse_value.client_id', 'parse_value.timestamp', 'parse_value.humidity','parse_value.pitch', 'parse_value.roll', 'parse_value.yaw', 'parse_value.count', 'parse_value.nested.temperature', 'parse_value.nested.pressure_test')

save_to_redshift = final_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "s3://innovator-island-bucket-ericacn/spark-redshift-cdc/checkpoint/") \
    .start()

save_to_redshift.awaitTermination()

