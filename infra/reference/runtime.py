"""Shared data plane and queue-only Fargate workers for the reference stack."""
import json
from typing import Any
from cfn_typed.intrinsics import Sub
import cfn_typed as cfn
from cfn_typed_sumo.types import dynamodb, ec2, ecs, iam, logs, s3, sqs


def assume(service: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]}


def policy(name: str, statements: list[Any]) -> iam.RolePolicyProperty:
    return {"PolicyName": name, "PolicyDocument": {"Version": "2012-10-17", "Statement": statements}}


def bucket() -> s3.Bucket:
    return s3.bucket(
        deletion_policy="Retain", update_replace_policy="Retain",
        PublicAccessBlockConfiguration={"BlockPublicAcls": True, "BlockPublicPolicy": True, "IgnorePublicAcls": True, "RestrictPublicBuckets": True},
        BucketEncryption={"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
    )


def config() -> Sub:
    return cfn.sub(json.dumps({
        "scripts": {"bucket": "${ScriptsBucket}"},
        "stores": {"Data": {"type": "s3", "bucket": "${DataBucket}", "prefix": "tenants/"}},
        "flow_logs": {"bucket": "${FlowLogsBucket}"},
        "db": {"table": "${StateTable}"},
        "queue": {"url": "${WorkQueue}"},
    }, separators=(",", ":")))


def resources() -> dict:
    scripts_arn = cfn.get_att("ScriptsBucket", "Arn")
    data_arn = cfn.get_att("DataBucket", "Arn")
    flow_arn = cfn.get_att("FlowLogsBucket", "Arn")
    table_arn = cfn.get_att("StateTable", "Arn")
    queue_arn = cfn.get_att("WorkQueue", "Arn")
    def objects(name: str) -> Sub:
        return cfn.sub("${" + name + ".Arn}/*")
    worker_permissions = [
        cfn.allow("s3:ListBucket", [scripts_arn, data_arn, flow_arn]),
        cfn.allow("s3:GetObject", [objects("ScriptsBucket"), objects("DataBucket"), objects("FlowLogsBucket")]),
        cfn.allow(["s3:PutObject", "s3:DeleteObject"], objects("DataBucket")),
        cfn.allow("s3:PutObject", objects("FlowLogsBucket")),
        cfn.allow(["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query"], table_arn),
        cfn.allow(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:SendMessage", "sqs:GetQueueAttributes"], queue_arn),
    ]
    control_permissions = [
        cfn.allow("s3:ListBucket", [scripts_arn, data_arn, flow_arn]),
        cfn.allow("s3:GetObject", [objects("ScriptsBucket"), objects("DataBucket"), objects("FlowLogsBucket")]),
        cfn.allow(["dynamodb:GetItem", "dynamodb:Query"], table_arn),
    ]
    return {
        "ScriptsBucket": bucket(), "DataBucket": bucket(), "FlowLogsBucket": bucket(),
        "StateTable": dynamodb.table(
            deletion_policy="Retain", update_replace_policy="Retain", BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            TimeToLiveSpecification={"AttributeName": "ttl", "Enabled": True},
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        ),
        "DeadLetterQueue": sqs.queue(SqsManagedSseEnabled=True, MessageRetentionPeriod=1209600),
        "WorkQueue": sqs.queue(SqsManagedSseEnabled=True, VisibilityTimeout=300, ReceiveMessageWaitTimeSeconds=20,
                               RedrivePolicy={"deadLetterTargetArn": cfn.get_att("DeadLetterQueue", "Arn"), "maxReceiveCount": 5}),
        "WorkerRole": iam.role(AssumeRolePolicyDocument=assume("ecs-tasks.amazonaws.com"), Policies=[policy("WorkerResources", worker_permissions)]),
        "ControlRole": iam.role(AssumeRolePolicyDocument=assume("lambda.amazonaws.com"),
            ManagedPolicyArns=[cfn.sub("arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")],
            Policies=[policy("ReadTenantFlows", control_permissions)]),
        "TaskExecutionRole": iam.role(AssumeRolePolicyDocument=assume("ecs-tasks.amazonaws.com"),
            ManagedPolicyArns=[cfn.sub("arn:${AWS::Partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy")]),
        "WorkerLogs": logs.log_group(RetentionInDays=30),
        "WorkerCluster": ecs.cluster(),
        "WorkerSecurityGroup": ec2.security_group(GroupDescription="Queue-only Integraal workers", VpcId=cfn.ref("VpcId"),
            SecurityGroupEgress=[{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0"}]),
        "WorkerTask": ecs.task_definition(
            RequiresCompatibilities=["FARGATE"], NetworkMode="awsvpc", Cpu="512", Memory="1024",
            ExecutionRoleArn=cfn.get_att("TaskExecutionRole", "Arn"), TaskRoleArn=cfn.get_att("WorkerRole", "Arn"),
            ContainerDefinitions=[{"Name": "worker", "Image": cfn.ref("WorkerImage"), "Essential": True,
                "EntryPoint": ["/bin/sh", "-c"],
                "Command": ['printf "%s" "$INTEGRAAL_CONFIG" > /tmp/integraal.json && exec /usr/local/bin/integraal-server --config /tmp/integraal.json'],
                "Environment": [{"Name": "INTEGRAAL_CONFIG", "Value": config()}],
                "LogConfiguration": {"LogDriver": "awslogs", "Options": {"awslogs-group": cfn.ref("WorkerLogs"), "awslogs-region": cfn.ref("AWS::Region"), "awslogs-stream-prefix": "worker"}}}],
        ),
        "WorkerService": ecs.service(Cluster=cfn.ref("WorkerCluster"), TaskDefinition=cfn.ref("WorkerTask"), LaunchType="FARGATE",
            DesiredCount=cfn.ref("WorkerCount"), NetworkConfiguration={"AwsvpcConfiguration": {"AssignPublicIp": "ENABLED", "SecurityGroups": [cfn.ref("WorkerSecurityGroup")], "Subnets": cfn.ref("PublicSubnets")}}),
        "SchedulerRole": iam.role(AssumeRolePolicyDocument=assume("scheduler.amazonaws.com"),
            Policies=[policy("SendTicks", [cfn.allow("sqs:SendMessage", queue_arn)])]),
        **{f"{name}Tick": {"Type": "AWS::Scheduler::Schedule", "Properties": {
            "FlexibleTimeWindow": {"Mode": "OFF"}, "ScheduleExpression": f"rate(1 {period})",
            "Target": {"Arn": queue_arn, "RoleArn": cfn.get_att("SchedulerRole", "Arn"),
                       "Input": json.dumps({"type": "system.tick", "every": every, "at": "<aws.scheduler.scheduled-time>"}, separators=(",", ":"))},
        }} for name, period, every in [("Minute", "minute", "minute"), ("Hour", "hour", "hour"), ("Day", "day", "day")]},
    }
