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

AccelerometerTrusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/accelerometer/trusted/"], "recurse": True},
    transformation_ctx="AccelerometerTrusted",
)

StepTrainerTrusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/step_trainer/trusted/"], "recurse": True},
    transformation_ctx="StepTrainerTrusted",
)

ML_Join = Join.apply(
    frame1=StepTrainerTrusted,
    frame2=AccelerometerTrusted,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="ML_Join",
)

df = ML_Join.toDF()
df_filtered = df.select("sensorReadingTime", "serialNumber", "distanceFromObject", "user", "x", "y", "z")
Cleaned_DynamicFrame = DynamicFrame.fromDF(df_filtered, glueContext, "Cleaned_DynamicFrame")

MachineLearningCurated_Sink = glueContext.getSink(
    path="s3://udacity-stedi-abhi/ML_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_Sink"
)
MachineLearningCurated_Sink.setCatalogInfo(
    catalogDatabase="stedi-project", 
    catalogTableName="machine_learning_curated"
)
MachineLearningCurated_Sink.setFormat("json")
MachineLearningCurated_Sink.writeFrame(Cleaned_DynamicFrame)

job.commit()
