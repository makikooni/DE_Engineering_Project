#=#=#=#=#=#=#=#=#=#=#=#=# IAM Role - Extract
resource "aws_iam_role" "extract_lambda_role" {
    name = "${local.extract_lambda_name}_role"
    description = "this role is for the extract lambda"
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

resource "aws_iam_role" "transform_lambda_role" {
    name = "${local.transform_lambda_name}_role"
    description = "this role is for the transform lambda"
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

resource "aws_iam_role" "load_lambda_role" {
    name = "${local.load_lambda_name}_role"
    description = "this role is for the load lambda"
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

/*
The policy setup for each of the services below follows the following patter:
  aws_iam_policy_document - create a document that defines permission and resources for the service
  aws_iam_policy - using the policy document, create a policy resource for the service
  aws_iam_role_policy_attachment - attach the policy resource for the service to the lambda role
*/

#=#=#=#=#=#=#=#=#=#=#=#=# SecretsManager Policy

data "aws_iam_policy_document" "sm_document" {
  # Allow the getting of secret values from the specified SecretManager resrouces
  statement {

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "${local.db_secrets_arn}",
      "${local.table_names_secrets_arn}",
      "${local.warehouse_db_secret_arn}",
      "${local.warehouse_table_names_arn}"
    ]
  }
}

resource "aws_iam_policy" "sm_policy_resource" {
    name = "sm_policy"
    policy = data.aws_iam_policy_document.sm_document.json
}

# Extract

resource "aws_iam_role_policy_attachment" "extract_lambda_sm_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy_resource.arn
}

# Transform

resource "aws_iam_role_policy_attachment" "transform_lambda_sm_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy_resource.arn
}

# Load

resource "aws_iam_role_policy_attachment" "load_lambda_sm_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy_resource.arn
}

#=#=#=#=#=#=#=#=#=#=#=#=# S3 Policy

data "aws_iam_policy_document" "s3_ingestion_document" {
  # Allow the uploading and downloading of an object, and listing of buckets from the S3 resources
  statement {

    actions = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.ingestion_zone.arn}/*", 
      "${aws_s3_bucket.ingestion_zone.arn}"
    ]
  }
}

resource "aws_iam_policy" "s3_ingestion_policy_resource" {
    name = "s3_ingestion_policy"
    policy = data.aws_iam_policy_document.s3_ingestion_document.json
}

data "aws_iam_policy_document" "s3_processed_document" {
  # Allow the uploading and downloading of an object, and listing of buckets from the S3 resources
  statement {

    actions = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    
    resources = [
      "${aws_s3_bucket.processed_zone.arn}/*",
      "${aws_s3_bucket.processed_zone.arn}"
    ]
  }
}

resource "aws_iam_policy" "s3_processed_policy_resource" {
    name = "s3_processed_policy"
    policy = data.aws_iam_policy_document.s3_processed_document.json
}

# Extract

resource "aws_iam_role_policy_attachment" "extract_lambda_s3_ingestion_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.s3_ingestion_policy_resource.arn
}

# Transform

resource "aws_iam_role_policy_attachment" "transform_lambda_s3_ingestion_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_ingestion_policy_resource.arn
}

resource "aws_iam_role_policy_attachment" "transform_lambda_s3_processed_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_processed_policy_resource.arn
}

# Load
resource "aws_iam_role_policy_attachment" "load_lambda_s3_processed_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.s3_processed_policy_resource.arn
}

#=#=#=#=#=#=#=#=#=#=#=#=# Cloudwatch Policy

# Extract

data "aws_iam_policy_document" "extract_lambda_cw_document" {
  # Allow the creation of a log group inside the specified account
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  
  # Allow the creation of a log stream and put log events in the specified log group  
  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.extract_lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "extract_lambda_cw_policy_resource" {
    name = "${local.extract_lambda_name}_cw_policy"
    policy = data.aws_iam_policy_document.extract_lambda_cw_document.json
}

resource "aws_iam_role_policy_attachment" "extract_lambda_cw_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.extract_lambda_cw_policy_resource.arn
}

# Transform

data "aws_iam_policy_document" "transform_lambda_cw_document" {
  # Allow the creation of a log group inside the specified account
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  
  # Allow the creation of a log stream and put log events in the specified log group  
  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.transform_lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "transform_lambda_cw_policy_resource" {
    name = "${local.transform_lambda_name}_cw_policy"
    policy = data.aws_iam_policy_document.transform_lambda_cw_document.json
}

resource "aws_iam_role_policy_attachment" "transform_lambda_cw_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.transform_lambda_cw_policy_resource.arn
}

# Load

data "aws_iam_policy_document" "load_lambda_cw_document" {
  # Allow the creation of a log group inside the specified account
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  
  # Allow the creation of a log stream and put log events in the specified log group  
  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.load_lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "load_lambda_cw_policy_resource" {
    name = "${local.load_lambda_name}_cw_policy"
    policy = data.aws_iam_policy_document.load_lambda_cw_document.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.load_lambda_cw_policy_resource.arn
}