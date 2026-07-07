import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)  


CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-stedi-abhi/customer/landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node1",
)

PrivacyFilter_node1693740592249 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0 or row["shareWithResearchAsOfDate"] is None)),
    transformation_ctx="PrivacyFilter_node1693740592249",
)

CustomerTrusted_node3 = glueContext.getSink(
    path="s3://udacity-stedi-abhi/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node3"
)
CustomerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi-project", 
    catalogTableName="customer_trusted"
)
CustomerTrusted_node3.setFormat("json")
CustomerTrusted_node3.writeFrame(PrivacyFilter_node1693740592249)

job.commit()
