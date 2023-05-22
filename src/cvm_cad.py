import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
# DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
#     database="glue_database",
#     table_name="csv",
#     transformation_ctx="DataCatalogtable_node1",
# )


df = spark.read\
    .option("delimiter", ";")\
    .option("header", "true")\
    .option("encoding", "latin-1")\
    .csv('s3://spinyleaks-lake/landing/csv/cad_fi.csv')


DataCatalogtable_node1 = DynamicFrame.fromDF(df, glueContext, "cadastro")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://spinyleaks-lake/bronze/cvm/cadastro/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="glue_database", catalogTableName="bronze_cvm_cad"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(DataCatalogtable_node1)
job.commit()
