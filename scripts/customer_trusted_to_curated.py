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

CustomerTrusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/customer/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted",
)

AccelerometerLanding = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/accelerometer/landing/"], "recurse": True},
    transformation_ctx="AccelerometerLanding",
)

PrivacyJoin = Join.apply(
    frame1=CustomerTrusted,
    frame2=AccelerometerLanding,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyJoin",
)

DropFields = ApplyMapping.apply(
    frame=PrivacyJoin,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="DropFields",
)

df = DropFields.toDF().dropDuplicates(['email'])
CustomerCurated = DynamicFrame.fromDF(df, glueContext, "CustomerCurated")

CustomerCurated_Sink = glueContext.getSink(
    path="s3://udacity-stedi-abhi/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_Sink"
)
CustomerCurated_Sink.setCatalogInfo(
    catalogDatabase="stedi-project", 
    catalogTableName="customer_curated"
)
CustomerCurated_Sink.setFormat("json")
CustomerCurated_Sink.writeFrame(CustomerCurated)

job.commit()
