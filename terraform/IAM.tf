# IAM Role for Lambda Functions
resource "aws_iam_role" "lambda_role" {
    name_prefix = "role_extract_data_to_ingestion_s3"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# SecretsManager Policy and Permissions
data "aws_iam_policy_document" "sm_document" {
  statement {

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "arn:aws:secretsmanager:eu-west-2:454963742860:secret:ingestion/db/credentials-y9n2MW",
      "arn:aws:secretsmanager:eu-west-2:454963742860:secret:ingestion/db/table-names-0OOOHO"
    ]
  }
}

resource "aws_iam_policy" "sm_policy" {
    name_prefix = "sm_policy_lambda"
    policy = data.aws_iam_policy_document.sm_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_sm_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.sm_policy.arn
}

# Ingestion S3 Policy and Permissions

data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:PutObject"]

    resources = [
      "${aws_s3_bucket.ingestion_zone.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "ingestion_s3_policy_extract_lambda"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}


# Cloudwatch Policy and Permissions

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
   
  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extract_lambda.function_name}:*"
    ]
  }
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw_policy_extract_lambda"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}