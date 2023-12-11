import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node customer_trusted
customer_trusted_node1702314745177 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1702314745177",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1702314780592 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1702314780592",
)

# Script generated for node SQL Query
SqlQuery242 = """
select * from customer_trusted
join accelerometer_trusted
on customer_trusted.email = accelerometer_trusted.user
"""
SQLQuery_node1702314855400 = sparkSqlQuery(
    glueContext,
    query=SqlQuery242,
    mapping={
        "customer_trusted": customer_trusted_node1702314745177,
        "accelerometer_trusted": accelerometer_trusted_node1702314780592,
    },
    transformation_ctx="SQLQuery_node1702314855400",
)

# Script generated for node Drop Fields
DropFields_node1702315000627 = DropFields.apply(
    frame=SQLQuery_node1702314855400,
    paths=["timestamp", "x", "y", "z", "user"],
    transformation_ctx="DropFields_node1702315000627",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702315611078 = DynamicFrame.fromDF(
    DropFields_node1702315000627.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702315611078",
)

# Script generated for node customer_curated
customer_curated_node1702315124881 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1702315124881",
)
customer_curated_node1702315124881.setCatalogInfo(
    catalogDatabase="os-stedi-database", catalogTableName="customer_curated"
)
customer_curated_node1702315124881.setFormat("json")
customer_curated_node1702315124881.writeFrame(DropDuplicates_node1702315611078)
job.commit()
