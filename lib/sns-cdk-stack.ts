import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sns from "aws-cdk-lib/aws-sns";
import * as glue from "aws-cdk-lib/aws-glue";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as events from "aws-cdk-lib/aws-events";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { aws_events_targets as targets } from "aws-cdk-lib";

export class SnsCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const envtype = this.node.tryGetContext("envtype");

    // IAM Roles and Policies
    const policy1 = new iam.PolicyStatement({
      actions: [
        "s3:*",
        "sns:*",
        "glue:*",
        "athena:*",
        "lambda:*",
        "eventbridge:*",
        "cloudwatch:PutMetricData",
      ],
      effect: iam.Effect.ALLOW,
      resources: ["*"],
    });

    const policy2 = new iam.PolicyStatement({
      actions: [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ],
      effect: iam.Effect.ALLOW,
      resources: ["arn:aws:logs:*:*:/aws-glue/*"],
    });

    const lambda_exec_role = new iam.Role(this, "role-lambda", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      roleName: "test-lambda-role",
      description: "Role for job failure failure notification",
    });

    // Adding Policy to Role
    lambda_exec_role.addToPolicy(policy1);
    lambda_exec_role.addToPolicy(policy2);

    const glue_exec_role = new iam.Role(this, "role-glue", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      roleName: "test-glue-role",
      description: "Role for job failure failure notification",
    });

    // Adding Policy to Role
    glue_exec_role.addToPolicy(policy1);
    glue_exec_role.addToPolicy(policy2);

    // SNS Topic
    const topic = new sns.Topic(this, "notification", {
      topicName: "RissanSNSTopic",
      displayName: "RissanSNSTopic",
    });

    // Multiple Subscriptions
    new sns.Subscription(this, "Subscription-1", {
      topic,
      endpoint: "ripattna@gmail.com",
      protocol: sns.SubscriptionProtocol.EMAIL,
    });

    new sns.Subscription(this, "Subscription-2", {
      topic,
      endpoint: "testno410@gmail.com",
      protocol: sns.SubscriptionProtocol.EMAIL,
    });

    // AWS Lambda Function
    const snsLambda = new lambda.Function(this, "sns-lambda", {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: "sns_lambda.lambda_handler",
      functionName: "sns-lambda",
      role: lambda_exec_role,
      code: lambda.Code.fromAsset(path.join(__dirname, "lambda_scripts_dir")),
      environment: {
        SNS_TOPIC_ARN: topic.topicArn,
        envtype: envtype,
      },
    });

    // AWS EventBridge
    const notificationRule = new events.Rule(this, "sns-notification", {
      ruleName: `sns-notification`,
      description: "This triger will at a time",
      eventPattern: {
        detailType: ["Glue Job State Change"],
        source: ["aws.glue"],
        detail: { state: ["FAILED"] },
      },
    });

    notificationRule.addTarget(new targets.LambdaFunction(snsLambda));

    // S3 Bucket
    var glue_script_bucket = `glue-script-bucket-${envtype}`;
    const glue_script_bucket_res = new s3.Bucket(this, "glue-script-bucket", {
      bucketName: glue_script_bucket,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      // versioned: true,
      // enforceSSL: true,
      // removalPolicy: RemovalPolicy.RETAIN,
      
      // serverAccessLogsBucket: s3.Bucket.fromBucketName(
      //   this,
      //   `glue-script-bucket-logging`,
      //   `glue-script-bucket-${envtype}`
      // ),
      // serverAccessLogsPrefix: "glue-script-bucket",
      // lifecycleRules:
    });
    glue_script_bucket_res.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ServicePrincipal("s3.amazonaws.com")],
        actions: ["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
        // actions: [
          // "s3:GetBucketLocation",
          // "s3:ListBucket",
          // "s3:ListBucketMultipartUploads",
          // "s3:ListMultipartUploadParts",
          // "s3:AbortMultipartUpload",
          //   "s3:GetObject",
          //   "s3:CreateBucket",
          //   "s3:PutObject",
        // ],
        resources: [
          "arn:aws:s3:::" + glue_script_bucket + "/*",
          "arn:aws:s3:::" + glue_script_bucket
        ],
      })
    );
    // Upload internal python script to S3
    const glue_etl_script = new s3deploy.BucketDeployment(
      this,
      "glue-python-script",
      {
        sources: [
          s3deploy.Source.asset(path.join(__dirname, "glue_scripts_dir")),
        ],
        destinationBucket: glue_script_bucket_res,
      }
    );

    // Glue Job
    const cfnJob1 = new glue.CfnJob(this, "MyCfnJob", {
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: "s3://" + glue_script_bucket + "/job-glue.py",
      },
      role: glue_exec_role.roleName,
      glueVersion: "3.0",
      numberOfWorkers: 2,
      workerType: "G.1X",
      maxRetries: 1,
      name: "job-glue-test",
      defaultArguments: {},
    });

    // Glue Scheduler
    const JobSchedule = new glue.CfnTrigger(this, "-job-schdeuler", {
      actions: [{ jobName: "job-glue-test" }],
      type: "SCHEDULED",
      name: "job-glue-test",
      schedule: "cron(0 20 * * ? *)",
      startOnCreation: true,
    });
  }
}

