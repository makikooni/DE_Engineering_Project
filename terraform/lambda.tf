resource "aws_lambda_function" "extract_data_to_ingestion_s3" {
  filename = "${path.module}/../function.zip"
  function_name = "extract_data_to_ingestion_s3"
  role = aws_iam_role.lambda_role.arn
  handler = "extract.extraction_lambda_handler"
  runtime = "python3.9"
}

# resource "aws_lambda_permission" "allow_extract_data_to_ingestion_s3" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.extract_data_to_ingestion_s3.function_name
#   principal = "s3.amazonaws.com"
#   source_arn = aws_s3_bucket.ingestion_zone.arn
#   source_account = data.aws_caller_identity.current.account_id
# }
