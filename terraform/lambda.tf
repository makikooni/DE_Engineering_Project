resource "aws_lambda_function" "extract_data_to_ingestion_s3" {
  filename = "${path.module}/../function.zip"
  function_name = "extract_data_to_ingestion_s3"
  role = aws_iam_role.lambda_role.arn
  handler = "extract.extraction_lambda_handler"
  runtime = "python3.9"
}
