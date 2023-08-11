import json
import logging
import boto3
import os

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def publish_to_sns(subject: str, message: str):
    """This method is used to publish the event"""

    try:
        sns_topic_arn = os.environ['SNS_TOPIC_ARN']
        # sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    except KeyError:
        sns_topic_arn = "SNS TOPIC NOT FOUND"

    logger.info(sns_topic_arn)
    client = boto3.client('sns')
    response = client.publish(
        TargetArn=sns_topic_arn,
        Message=message,
        Subject=subject)

    return response


def final_status(glue_job_name,glue_job_state,job_run_id,failure_message,job_failure_time,job_id):
    """This method returns the message and subject which is arquire in publish_to_sns():"""

    subject = f"""AWS Glue job has failed: {glue_job_name}""" 

    message = f"""
    Dear Team

    This is an auto generated email triggered to inform you,The AWS Glue job '{glue_job_name}' has failed.
    Please take immediate attendentation to it.

    --------------------------------------------------------------------------------------------------------------------
    Failed Glue Job Details:
    --------------------------------------------------------------------------------------------------------------------
    Job Name:      {glue_job_name}
    Job State:     {glue_job_state}
    Glue Job_Id:   {job_id}
    Job Run_Id:    {job_run_id}
    Failure Time:  {job_failure_time}
    Failure Error: {failure_message}
    --------------------------------------------------------------------------------------------------------------------

    We suggest you investigate the issue and take necessary action to rectify it promptly.

    Regards,
    Monitoring Team"""

    logger.info(subject)
    logger.info(message)

    publish_to_sns(subject=subject, message=message)


# Define Lambda function
def lambda_handler(event, context):
    """The main method of the program:"""
    logger.info(f"# INITIATED BY EVENT: \n{event['detail']}")

    # Define variables based on the event
    glue_job_name = event['detail']['jobName']
    glue_job_state = event['detail']['state']
    job_run_id = event['detail']['jobRunId']
    failure_message = event['detail']['message']
    job_failure_time = event['time']
    job_id = event['id']

    # Only send SNS notification if the job starts with "job-" and if the job did a retries of 1 time:

    if event['detail']['jobName'].startswith('job-') & event['detail']['jobRunId'].endswith('_attempt_1'):
        logger.info(f'# GLUE JOB FAILED: {glue_job_name}')
        
        final_status(glue_job_name,glue_job_state,job_run_id,failure_message,job_failure_time,job_id)

