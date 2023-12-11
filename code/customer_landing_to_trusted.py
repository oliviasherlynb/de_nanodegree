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
customer_landing_node1702313665236 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1702313665236",
)

# Script generated for node SQL Query
SqlQuery282 = """
select * from customer_landing where customer_landing.shareWithResearchAsOfDate is not null
"""
SQLQuery_node1702313573885 = sparkSqlQuery(
    glueContext,
    query=SqlQuery282,
    mapping={"customer_landing": customer_landing_node1702313665236},
    transformation_ctx="SQLQuery_node1702313573885",
)

# Script generated for node customer_trusted
customer_trusted_node1702313850369 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1702313850369",
)
customer_trusted_node1702313850369.setCatalogInfo(
    catalogDatabase="os-stedi-database", catalogTableName="customer_trusted"
)
customer_trusted_node1702313850369.setFormat("json")
customer_trusted_node1702313850369.writeFrame(SQLQuery_node1702313573885)
job.commit()
