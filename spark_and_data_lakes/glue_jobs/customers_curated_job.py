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

# Script generated for node accelerometer_landing
accelerometer_landing_node1739626970999 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1739626970999")

# Script generated for node customer_landing
customer_landing_node1739627300696 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1739627300696")

# Script generated for node distinct_user
SqlQuery3857 = '''
SELECT distinct user from myDataSource;
'''
distinct_user_node1739627195386 = sparkSqlQuery(glueContext, query = SqlQuery3857, mapping = {"myDataSource":accelerometer_landing_node1739626970999}, transformation_ctx = "distinct_user_node1739627195386")

# Script generated for node share_with_research
SqlQuery3856 = '''
select * from myDataSource WHERE sharewithresearchasofdate != 0;
'''
share_with_research_node1739628375186 = sparkSqlQuery(glueContext, query = SqlQuery3856, mapping = {"myDataSource":customer_landing_node1739627300696}, transformation_ctx = "share_with_research_node1739628375186")

# Script generated for node Join
Join_node1739628163355 = Join.apply(frame1=distinct_user_node1739627195386, frame2=share_with_research_node1739628375186, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1739628163355")

# Script generated for node GDPR Compliance
GDPRCompliance_node1739627488793 = DropFields.apply(frame=Join_node1739628163355, paths=["birthDay", "customerName", "email", "phone"], transformation_ctx="GDPRCompliance_node1739627488793")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=GDPRCompliance_node1739627488793, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739626470889", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1739627870772 = glueContext.getSink(path="s3://stedi-lake-house-nd027/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="customer_curated_node1739627870772")
customer_curated_node1739627870772.setCatalogInfo(catalogDatabase="steadi",catalogTableName="customer_curated")
customer_curated_node1739627870772.setFormat("json")
customer_curated_node1739627870772.writeFrame(GDPRCompliance_node1739627488793)
job.commit()
