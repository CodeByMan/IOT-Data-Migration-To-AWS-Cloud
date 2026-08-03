from aws_cdk import CfnParameter, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from constructs import Construct


class TopicAdminPrerequisitesStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            **kwargs,
        )

        msk_security_group_id = CfnParameter(
            self,
            "MskSecurityGroupId",
            type="AWS::EC2::SecurityGroup::Id",
        )

        bastion_security_group_id = CfnParameter(
            self,
            "BastionSecurityGroupId",
            type="AWS::EC2::SecurityGroup::Id",
        )

        bastion_role_name = CfnParameter(
            self,
            "BastionRoleName",
            type="String",
        )

        scram_secret_arn = CfnParameter(
            self,
            "ScramSecretArn",
            type="String",
        )

        scram_kms_key_arn = CfnParameter(
            self,
            "ScramKmsKeyArn",
            type="String",
        )

        ec2.CfnSecurityGroupIngress(
            self,
            "BastionToMskScramIngress",
            group_id=(
                msk_security_group_id
                .value_as_string
            ),
            ip_protocol="tcp",
            from_port=9096,
            to_port=9096,
            source_security_group_id=(
                bastion_security_group_id
                .value_as_string
            ),
            description=(
                "Allow the private Bastion "
                "host to administer MSK "
                "through SASL/SCRAM"
            ),
        )

        iam.CfnPolicy(
            self,
            "BastionMskScramAccessPolicy",
            policy_name=(
                "hackathon-bastion-"
                "msk-scram-access"
            ),
            roles=[
                bastion_role_name
                .value_as_string
            ],
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": (
                            "ReadMskScramSecret"
                        ),
                        "Effect": "Allow",
                        "Action": [
                            (
                                "secretsmanager:"
                                "GetSecretValue"
                            ),
                            (
                                "secretsmanager:"
                                "DescribeSecret"
                            ),
                        ],
                        "Resource": (
                            scram_secret_arn
                            .value_as_string
                        ),
                    },
                    {
                        "Sid": (
                            "DecryptMskScramSecret"
                        ),
                        "Effect": "Allow",
                        "Action": [
                            "kms:Decrypt",
                            "kms:DescribeKey",
                        ],
                        "Resource": (
                            scram_kms_key_arn
                            .value_as_string
                        ),
                    },
                ],
            },
        )
