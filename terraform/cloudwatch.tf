resource "aws_cloudwatch_event_rule" "extraction_schedule" {
    name = "extraction_schedule"
    description = "Triggers at specified interval"
    schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "extract_schedule_extraction_lambda" {
    rule = aws_cloudwatch_event_rule.extraction_schedule.name
    target_id = aws_lambda_function.extract_lambda.function_name
    arn = aws_lambda_function.extract_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_execution_extract_lambda" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.extraction_schedule.arn
}
