import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1697816018967 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1697816018967",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1697816017677 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1697816017677",
)

# Script generated for node Join
Join_node1697816115920 = Join.apply(
    frame1=AWSGlueDataCatalog_node1697816018967,
    frame2=accelerometer_trusted_node1697816017677,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1697816115920",
)

# Script generated for node SQL Query
SqlQuery1640 = """
SELECT DISTINCT serialnumber, sharewithpublicasofdate, birthday, registrationdate ,sharewithresearchasofdate,customername ,email ,lastupdatedate, phone, sharewithfriendsasofdate 
from myDataSource

"""
SQLQuery_node1697816153792 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1640,
    mapping={"myDataSource": Join_node1697816115920},
    transformation_ctx="SQLQuery_node1697816153792",
)

# Script generated for node customer_Curated
customer_Curated_node1697816205430 = glueContext.getSink(
    path="s3://stedi-hb-lakehouse/src/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="customer_Curated_node1697816205430",
)
customer_Curated_node1697816205430.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="customer_Curated"
)
customer_Curated_node1697816205430.setFormat("json")
customer_Curated_node1697816205430.writeFrame(SQLQuery_node1697816153792)
job.commit()
