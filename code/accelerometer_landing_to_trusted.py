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
accelerometer_landing_node1702314170925 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1702314170925",
)

# Script generated for node customer_trusted
customer_trusted_node1702314210825 = glueContext.create_dynamic_frame.from_catalog(
    database="os-stedi-database",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1702314210825",
)

# Script generated for node SQL Query
SqlQuery224 = """
select accelerometer_landing.user, accelerometer_landing.timestamp, accelerometer_landing.x, accelerometer_landing.y, accelerometer_landing.z from accelerometer_landing
join customer_trusted 
on customer_trusted.email = accelerometer_landing.user

"""
SQLQuery_node1702314232603 = sparkSqlQuery(
    glueContext,
    query=SqlQuery224,
    mapping={
        "customer_trusted": customer_trusted_node1702314210825,
        "accelerometer_landing": accelerometer_landing_node1702314170925,
    },
    transformation_ctx="SQLQuery_node1702314232603",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1702314497712 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1702314497712",
)
accelerometer_trusted_node1702314497712.setCatalogInfo(
    catalogDatabase="os-stedi-database", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1702314497712.setFormat("json")
accelerometer_trusted_node1702314497712.writeFrame(SQLQuery_node1702314232603)
job.commit()
