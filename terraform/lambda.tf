resource "aws_lambda_function" "extract_data_to_ingestion_s3" {
  filename = "${path.module}/../function.zip"
  function_name = "extract_data_to_ingestion_s3"
  role = aws_iam_role.lambda_role.arn
  handler = "extract.extraction_lambda_handler"
  runtime = "python3.9"
}

resource "aws_lambda_function" "load_data_to_warehouse" {
  filename = "${path.module}/../function.zip"
  function_name = "load_data_to_warehouse"
  role = aws_iam_role.lambda_role.arn
  handler = "extract.extraction_lambda_handler"  #change afterwards for new handler
  runtime = "python3.9"
}

