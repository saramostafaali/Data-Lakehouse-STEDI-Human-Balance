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

# Script generated for node accelerometer_landing
accelerometer_landing_node1697812626882 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1697812626882",
)

# Script generated for node customer_trusted
customer_trusted_node1697812628665 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1697812628665",
)

# Script generated for node Join
Join_node1697813388440 = Join.apply(
    frame1=customer_trusted_node1697812628665,
    frame2=accelerometer_landing_node1697812626882,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1697813388440",
)

# Script generated for node SQL Query
SqlQuery1497 = """
select DISTINCT user,  timestamp, x,  y, z from myDataSource
"""
SQLQuery_node1697814023887 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1497,
    mapping={"myDataSource": Join_node1697813388440},
    transformation_ctx="SQLQuery_node1697814023887",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1697812808158 = glueContext.getSink(
    path="s3://stedi-hb-lakehouse/src/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1697812808158",
)
accelerometer_trusted_node1697812808158.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1697812808158.setFormat("json")
accelerometer_trusted_node1697812808158.writeFrame(SQLQuery_node1697814023887)
job.commit()
