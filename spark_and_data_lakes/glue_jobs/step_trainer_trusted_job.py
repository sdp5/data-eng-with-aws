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
accelerometer_landing_node1739629744538 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1739629744538")

# Script generated for node customer_landing
customer_landing_node1739629883225 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1739629883225")

# Script generated for node step_trainer_landing
step_trainer_landing_node1739629993325 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1739629993325")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * FROM stepTrainerData
JOIN customerData on stepTrainerData.serialnumber = customerData.serialnumber
WHERE stepTrainerData.sensorreadingtime IN (
    SELECT distinct timestamp from accelerometerData
)
AND customerData.sharewithresearchasofdate != 0;
'''
SQLQuery_node1739630210468 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customerData":customer_landing_node1739629883225, "accelerometerData":accelerometer_landing_node1739629744538, "stepTrainerData":step_trainer_landing_node1739629993325}, transformation_ctx = "SQLQuery_node1739630210468")

# Script generated for node Drop Fields
DropFields_node1739630473234 = DropFields.apply(frame=SQLQuery_node1739630210468, paths=["customerName", "email", "phone", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1739630473234")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1739630473234, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739628911649", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1739630523171 = glueContext.getSink(path="s3://stedi-lake-house-nd027/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1739630523171")
step_trainer_trusted_node1739630523171.setCatalogInfo(catalogDatabase="steadi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1739630523171.setFormat("json")
step_trainer_trusted_node1739630523171.writeFrame(DropFields_node1739630473234)
job.commit()
