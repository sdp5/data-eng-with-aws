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
customer_landing_node1739632130153 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1739632130153")

# Script generated for node step_trainer_landing
step_trainer_landing_node1739632296859 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-nd027/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1739632296859")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1739631999994 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1739631999994")

# Script generated for node share_with_research
SqlQuery0 = '''
select * from myDataSource WHERE sharewithresearchasofdate != 0;

'''
share_with_research_node1739632162949 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1739632130153}, transformation_ctx = "share_with_research_node1739632162949")

# Script generated for node SQL JOIN Query
SqlQuery1 = '''
SELECT * FROM stepTrainerData
JOIN accelerometerData on stepTrainerData.sensorreadingtime = accelerometerData.timestamp
JOIN customerData on stepTrainerData.serialnumber = customerData.serialnumber;

'''
SQLJOINQuery_node1739632350828 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"stepTrainerData":step_trainer_landing_node1739632296859, "customerData":share_with_research_node1739632162949, "accelerometerData":accelerometer_trusted_node1739631999994}, transformation_ctx = "SQLJOINQuery_node1739632350828")

# Script generated for node Protect PII
ProtectPII_node1739636833615 = DropFields.apply(frame=SQLJOINQuery_node1739632350828, paths=["customerName", "email", "phone", "birthDay"], transformation_ctx="ProtectPII_node1739636833615")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=ProtectPII_node1739636833615, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739626470889", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1739632526331 = glueContext.getSink(path="s3://stedi-lake-house-nd027/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1739632526331")
machine_learning_curated_node1739632526331.setCatalogInfo(catalogDatabase="steadi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1739632526331.setFormat("json")
machine_learning_curated_node1739632526331.writeFrame(ProtectPII_node1739636833615)
job.commit()
