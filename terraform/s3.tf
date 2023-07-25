resource "aws_s3_bucket" "ingestion_zone" {
  bucket = "ingestion-va-052023"
}

resource "aws_s3_bucket" "processed_zone" {
  bucket = "processed-va-052023"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.ingestion_zone.bucket
  key = "extract_data_to_ingestion_s3/function.zip"
  source = "${path.module}/../function.zip"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion_zone.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.extract_data_to_ingestion_s3.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_extract_data_to_ingestion_s3]
}