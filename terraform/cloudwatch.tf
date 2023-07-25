resource "aws_cloudwatch_event_rule" "every_one_minute" {
    name = "every_one_minute"
    description = "Triggers every one minute"
    schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "extract_data_to_ingestion_every_one_minute" {
    rule = aws_cloudwatch_event_rule.every_one_minute.name
    target_id = aws_lambda_function.extract_data_to_ingestion_s3.function_name
    arn = aws_lambda_function.extract_data_to_ingestion_s3.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_extract_data_to_ingestion" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_data_to_ingestion_s3.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.every_one_minute.arn
}