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

# Script generated for node customer_curated
customer_curated_node1702315961569 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1702315961569",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1702315988918 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1702315988918",
)

# Script generated for node SQL Query
SqlQuery241 = """
select stl.sensorreadingtime, stl.serialnumber, stl.distancefromobject
from stl
join cc on cc.serialnumber = stl.serialnumber

"""
SQLQuery_node1702316020603 = sparkSqlQuery(
    glueContext,
    query=SqlQuery241,
    mapping={
        "cc": customer_curated_node1702315961569,
        "stl": step_trainer_landing_node1702315988918,
    },
    transformation_ctx="SQLQuery_node1702316020603",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1702316202993 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1702316202993",
)
step_trainer_trusted_node1702316202993.setCatalogInfo(
    catalogDatabase="os-stedi-database", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1702316202993.setFormat("json")
step_trainer_trusted_node1702316202993.writeFrame(SQLQuery_node1702316020603)
job.commit()
