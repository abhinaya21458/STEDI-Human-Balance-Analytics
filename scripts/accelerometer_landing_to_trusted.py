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

AccelerometerLanding = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/accelerometer/landing/"], "recurse": True},
    transformation_ctx="AccelerometerLanding",
)

CustomerTrusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/customer/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted",
)

Join_node = Join.apply(
    frame1=AccelerometerLanding,
    frame2=CustomerTrusted,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node",
)

df = Join_node.toDF()
df_filtered = df.select("user", "timestamp", "x", "y", "z")
Cleaned_DynamicFrame = DynamicFrame.fromDF(df_filtered, glueContext, "Cleaned_DynamicFrame")

AccelerometerTrusted_Sink = glueContext.getSink(
    path="s3://udacity-stedi-abhi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_Sink"
)
AccelerometerTrusted_Sink.setCatalogInfo(
    catalogDatabase="stedi-project", 
    catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_Sink.setFormat("json")
AccelerometerTrusted_Sink.writeFrame(Cleaned_DynamicFrame)
job.commit()
