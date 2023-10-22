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

# Script generated for node customer_landing
customer_landing_node1697811241134 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node1697811241134",
)

# Script generated for node SQL Query
SqlQuery1582 = """
SELECT DISTINCT serialnumber, sharewithpublicasofdate, birthday, registrationdate ,sharewithresearchasofdate,customername ,email ,lastupdatedate, phone, sharewithfriendsasofdate
FROM customer_landing
WHERE sharewithresearchasofdate != 0
"""
SQLQuery_node1697811320197 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1582,
    mapping={"customer_landing": customer_landing_node1697811241134},
    transformation_ctx="SQLQuery_node1697811320197",
)

# Script generated for node customer_trusted
customer_trusted_node1697811417191 = glueContext.getSink(
    path="s3://stedi-hb-lakehouse/src/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1697811417191",
)
customer_trusted_node1697811417191.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="customer_trusted"
)
customer_trusted_node1697811417191.setFormat("json")
customer_trusted_node1697811417191.writeFrame(SQLQuery_node1697811320197)
job.commit()
