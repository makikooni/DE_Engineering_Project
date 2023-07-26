resource "aws_sns_topic" "extract_errors" {
    name = "extract-errors-topic"
}

resource "aws_sns_topic_subscription" "extract_errors_email_target" {
    topic_arn = aws_sns_topic.extract_errors.arn
    protocol = "email"
    endpoint = "david.geddes.de-202307@northcoders.net"
}

resource "aws_cloudwatch_event_rule" "scheduler" {
    name_prefix = "extraction-scheduler-"
    schedule_expression = "rate(1 minute)"
}

resource "aws_lambda_permission" "allow_scheduler" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_data_to_ingestion_s3.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_lambda_function.extract_data_to_ingestion_s3.arn
}
