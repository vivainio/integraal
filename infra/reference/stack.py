"""Reference CloudFormation for the tenant-scoped control-plane HTTP API."""
import cfn_typed as cfn
from cfn_typed_sumo.types import apigatewayv2 as api, lambda_
import runtime


def build() -> cfn.Template:
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Integraal reference hosting stack",
        "Parameters": {
            "UiOrigin": {"Type": "String", "Default": "http://localhost:5173", "Description": "Origin allowed to call the control API from a browser"},
            "JwtIssuer": {"Type": "String"},
            "JwtAudience": {"Type": "String"},
            "AccessScope": {"Type": "String", "Description": "OAuth access-token scope required for control-plane routes"},
            "VpcId": {"Type": "AWS::EC2::VPC::Id", "Description": "VPC with public subnets and an internet gateway"},
            "PublicSubnets": {"Type": "List<AWS::EC2::Subnet::Id>", "Description": "Public subnets in the selected VPC"},
            "WorkerImage": {"Type": "String", "Description": "Container image with /bin/sh and /usr/local/bin/integraal-server"},
            "WorkerCount": {"Type": "Number", "Default": 1, "MinValue": 0, "MaxValue": 20},
            "CodeBucket": {"Type": "String", "Description": "S3 bucket containing the Lambda zip"},
            "CodeKey": {"Type": "String", "Description": "S3 key of the zip containing the control-plane bootstrap"},
        },
        "Resources": {
            **runtime.resources(),
            "ControlFunction": lambda_.function(
                Code={"S3Bucket": cfn.ref("CodeBucket"), "S3Key": cfn.ref("CodeKey")},
                Handler="bootstrap", Runtime="provided.al2023", Architectures=["arm64"],
                Role=cfn.get_att("ControlRole", "Arn"), Timeout=30, MemorySize=512,
                Environment={"Variables": {"INTEGRAAL_CONTROL_CONFIG": runtime.config()}},
            ),
            "ControlApi": api.api(Name="integraal-control-plane", ProtocolType="HTTP", CorsConfiguration={"AllowOrigins": [cfn.ref("UiOrigin")], "AllowHeaders": ["authorization", "content-type"], "AllowMethods": ["GET", "OPTIONS"], "MaxAge": 300}),
            "JwtAuthorizer": api.authorizer(
                ApiId=cfn.ref("ControlApi"), AuthorizerType="JWT", Name="tenant-jwt",
                IdentitySource=["$request.header.Authorization"],
                JwtConfiguration={"Issuer": cfn.ref("JwtIssuer"), "Audience": [cfn.ref("JwtAudience")]},
            ),
            "LambdaIntegration": api.integration(
                ApiId=cfn.ref("ControlApi"), IntegrationType="AWS_PROXY",
                IntegrationUri=cfn.get_att("ControlFunction", "Arn"), PayloadFormatVersion="2.0",
            ),
            "FlowsRoute": api.route(
                ApiId=cfn.ref("ControlApi"), RouteKey="GET /flows", AuthorizationType="JWT",
                AuthorizerId=cfn.ref("JwtAuthorizer"), AuthorizationScopes=[cfn.ref("AccessScope")],
                Target=cfn.sub("integrations/${LambdaIntegration}"),
            ),
            "FlowRoute": api.route(
                ApiId=cfn.ref("ControlApi"), RouteKey="GET /flows/{trace}", AuthorizationType="JWT",
                AuthorizerId=cfn.ref("JwtAuthorizer"), AuthorizationScopes=[cfn.ref("AccessScope")],
                Target=cfn.sub("integrations/${LambdaIntegration}"),
            ),
            "FlowLogsRoute": api.route(
                ApiId=cfn.ref("ControlApi"), RouteKey="GET /flows/{trace}/logs", AuthorizationType="JWT",
                AuthorizerId=cfn.ref("JwtAuthorizer"), AuthorizationScopes=[cfn.ref("AccessScope")],
                Target=cfn.sub("integrations/${LambdaIntegration}"),
            ),
            "DefaultStage": api.stage(ApiId=cfn.ref("ControlApi"), StageName="$default", AutoDeploy=True),
            "GatewayInvokePermission": lambda_.permission(
                Action="lambda:InvokeFunction", FunctionName=cfn.ref("ControlFunction"),
                Principal="apigateway.amazonaws.com",
                SourceArn=cfn.sub("arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${ControlApi}/$default/GET/flows*"),
            ),
        },
        "Outputs": {
            "ApiUrl": {"Value": cfn.sub("https://${ControlApi}.execute-api.${AWS::Region}.${AWS::URLSuffix}")},
            "ScriptsBucket": {"Value": cfn.ref("ScriptsBucket")}, "DataBucket": {"Value": cfn.ref("DataBucket")},
            "FlowLogsBucket": {"Value": cfn.ref("FlowLogsBucket")}, "StateTable": {"Value": cfn.ref("StateTable")},
            "WorkQueueUrl": {"Value": cfn.ref("WorkQueue")}, "WorkerCluster": {"Value": cfn.ref("WorkerCluster")},
        },
    }


if __name__ == "__main__":
    print(cfn.to_json(build()))
