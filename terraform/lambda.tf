resource "aws_lambda_function" "extract_data_to_ingestion_s3" {
  filename = "${path.module}/../function.zip"
  function_name = "extract_data_to_ingestion_s3"
  role = aws_iam_role.lambda_role.arn
  handler = "../src/extract/extract.py"
  runtime = "python3.9"
}

resource "aws_lambda_permission" "allow_extract_data_to_ingestion_s3" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_data_to_ingestion_s3.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_zone.arn
  source_account = data.aws_caller_identity.current.account_id
}











# resource "aws_lambda_function" "extract_data_to_ingestion" {
#     filename = "extract_data_to_ingestion.zip"
#     function_name = "extract_data"
#     # role = "arn:aws:iam::424242:role/something"
#     handler = "extract.handler"
# }

# resource "aws_cloudwatch_event_rule" "every_five_minutes" {
#     name = "every-five-minutes"
#     description = "Triggers every five minutes"
#     schedule_expression = "rate(5 minutes)"
# }

# resource "aws_cloudwatch_event_target" "extract_data_to_ingestion_every_five_minutes" {
#     rule = aws_cloudwatch_event_rule.every_five_minutes.name
#     target_id = "extract_data_to_ingestion"
#     arn = aws_lambda_function.extract_data_to_ingestion.arn
# }

# resource "aws_lambda_permission" "allow_cloudwatch_to_call_extract_data_to_ingestion" {
#     statement_id = "AllowExecutionFromCloudWatch"
#     action = "lambda:InvokeFunction"
#     function_name = aws_lambda_function.extract_data_to_ingestion.function_name
#     principal = "events.amazonaws.com"
#     source_arn = aws_cloudwatch_event_rule.every_five_minutes.arn
# }