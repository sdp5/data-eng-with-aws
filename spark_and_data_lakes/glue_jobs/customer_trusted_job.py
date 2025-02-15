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
customer_landing_node1739613447996 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1739613447996")

# Script generated for node share_with_research
SqlQuery0 = '''
SELECT * FROM myDataSource WHERE sharewithresearchasofdate != 0;
'''
share_with_research_node1739613556176 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1739613447996}, transformation_ctx = "share_with_research_node1739613556176")

# Script generated for node Protected PII
ProtectedPII_node1739635940862 = DropFields.apply(frame=share_with_research_node1739613556176, paths=["customerName", "phone", "birthDay", "email"], transformation_ctx="ProtectedPII_node1739635940862")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=ProtectedPII_node1739635940862, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739613413898", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1739613629614 = glueContext.getSink(path="s3://stedi-lake-house-nd027/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1739613629614")
customer_trusted_node1739613629614.setCatalogInfo(catalogDatabase="steadi",catalogTableName="customer_trusted")
customer_trusted_node1739613629614.setFormat("json")
customer_trusted_node1739613629614.writeFrame(ProtectedPII_node1739635940862)
job.commit()
