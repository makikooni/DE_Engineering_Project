#=#=#=#=#=#=#=#=#=#=#=#=# Cloudwatch Event

# Extract

resource "aws_cloudwatch_event_rule" "extraction_schedule" {
    name = "extraction_schedule"
    description = "Triggers the extract lambda at specified interval"
    schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "extract_schedule_extraction_lambda" {
    rule = aws_cloudwatch_event_rule.extraction_schedule.name
    target_id = aws_lambda_function.extract_lambda.function_name
    arn = aws_lambda_function.extract_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_invoke_extract_lambda" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.extraction_schedule.arn
}

# Load

resource "aws_cloudwatch_event_rule" "load_schedule" {
    name = "load_schedule"
    description = "Triggers the load lambda at specified interval"
    schedule_expression = "rate(20 minutes)"
}

resource "aws_cloudwatch_event_target" "load_schedule_load_lambda" {
    rule = aws_cloudwatch_event_rule.load_schedule.name
    target_id = aws_lambda_function.load_lambda.function_name
    arn = aws_lambda_function.load_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_invoke_load_lambda" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.load_lambda.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.load_schedule.arn
}