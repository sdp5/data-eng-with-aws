import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_landing
customer_landing_node1739616373275 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1739616373275")

# Script generated for node accelerometer_landing
accelerometer_landing_node1739616340139 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1739616340139")

# Script generated for node customer_trusted
SqlQuery477 = '''
SELECT * FROM myDataSource WHERE sharewithresearchasofdate != 0
'''
customer_trusted_node1739616541320 = sparkSqlQuery(glueContext, query = SqlQuery477, mapping = {"myDataSource":customer_landing_node1739616373275}, transformation_ctx = "customer_trusted_node1739616541320")

# Script generated for node Join
Join_node1739616628644 = Join.apply(frame1=customer_trusted_node1739616541320, frame2=accelerometer_landing_node1739616340139, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1739616628644")

# Script generated for node Drop Fields
DropFields_node1739624347194 = DropFields.apply(frame=Join_node1739616628644, paths=["serialNumber", "birthDay", "shareWithPublicAsOfDate", "shareWithResearchAsOfDate", "registrationDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone"], transformation_ctx="DropFields_node1739624347194")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1739624347194, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739615414072", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1739616682423 = glueContext.getSink(path="s3://stedi-lake-house-nd027/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1739616682423")
accelerometer_trusted_node1739616682423.setCatalogInfo(catalogDatabase="steadi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1739616682423.setFormat("glueparquet", compression="snappy")
accelerometer_trusted_node1739616682423.writeFrame(DropFields_node1739624347194)
job.commit()
