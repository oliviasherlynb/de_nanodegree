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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1702316592091 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1702316592091",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1702316556350 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1702316556350",
)

# Script generated for node SQL Query
SqlQuery241 = """
select * from act
join stt on act.timestamp = stt.sensorreadingtime
"""
SQLQuery_node1702316622017 = sparkSqlQuery(
    glueContext,
    query=SqlQuery241,
    mapping={
        "stt": step_trainer_trusted_node1702316556350,
        "act": accelerometer_trusted_node1702316592091,
    },
    transformation_ctx="SQLQuery_node1702316622017",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1702316727600 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/final_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1702316727600",
)
machine_learning_curated_node1702316727600.setCatalogInfo(
    catalogDatabase="os-stedi-database", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1702316727600.setFormat("json")
machine_learning_curated_node1702316727600.writeFrame(SQLQuery_node1702316622017)
job.commit()
