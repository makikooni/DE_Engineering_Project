#=#=#=#=#=#=#=#=#=#=#=#=# S3 Buckets

# Extract

resource "aws_s3_bucket" "ingestion_zone" {
  bucket = "ingestion-${local.suffix}"
  force_destroy = true
}

#   permission for ingestion_zone (S3) to invoke transform lambda
resource "aws_lambda_permission" "allow_ingestion_s3_invoke_transform_lambda" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.transform_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingestion_zone.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "ingestion_s3_notification" {
  bucket = aws_s3_bucket.ingestion_zone.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "ExtractHistory/"
    filter_suffix       = ".txt"
  }

  depends_on = [aws_lambda_permission.allow_ingestion_s3_invoke_transform_lambda]
}

# Transform

resource "aws_s3_bucket" "processed_zone" {
  bucket = "processed-${local.suffix}"
  force_destroy = true
}