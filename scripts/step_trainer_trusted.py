import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CustomerCurated = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/customer/curated/"], "recurse": True},
    transformation_ctx="CustomerCurated",
)

StepTrainerLanding = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/step_trainer/landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding",
)

PrivacyJoin = Join.apply(
    frame1=StepTrainerLanding,
    frame2=CustomerCurated,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="PrivacyJoin",
)

DropFields = ApplyMapping.apply(
    frame=PrivacyJoin,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="DropFields",
)

StepTrainerTrusted_Sink = glueContext.getSink(
    path="s3://udacity-stedi-abhi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_Sink"
)
StepTrainerTrusted_Sink.setCatalogInfo(
    catalogDatabase="stedi-project", 
    catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_Sink.setFormat("json")
StepTrainerTrusted_Sink.writeFrame(DropFields)

job.commit()
