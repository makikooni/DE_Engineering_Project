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
    schedule_expression = "rate(3 minutes)"
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



resource "aws_cloudwatch_log_metric_filter" "info_trigger" {
    name = "Info"
    pattern = "INFO"
    log_group_name = "/aws/lambda/${aws_lambda_function.extract_data_to_ingestion_s3.function_name}"

    metric_transformation {
        name = "InfoSum"
        namespace = "InfoLambdaMetrics"
        value = "1"
    }
}
resource "aws_cloudwatch_metric_alarm" "alert_info" {
    alarm_name = "test-info"
    metric_name = "InfoSum"
    namespace = "InfoLambdaMetrics"
    alarm_description = "Info has been logged in the log group"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    statistic = "Sum"
    evaluation_periods = 1
    period = 60
    threshold = 2
    alarm_actions = [aws_sns_topic.extract_errors.arn]
    ok_actions = [aws_sns_topic.extract_errors.arn]
}



resource "aws_cloudwatch_log_metric_filter" "name_error" {
    name = "NameErrorFilter"
    pattern = "NameError"
    log_group_name = "/aws/lambda/${aws_lambda_function.extract_data_to_ingestion_s3.function_name}"

    metric_transformation {
        name = "NameErrorSum"
        namespace = "NameErrorLambdaMetrics"
        value = "1"
    }
}
resource "aws_cloudwatch_metric_alarm" "alert_name_error" {
    alarm_name = "test-name-error"
    metric_name = "NameErrorSum"
    namespace = "NameErrorLambdaMetrics"
    alarm_description = "A NameError has occurred."
    comparison_operator = "GreaterThanOrEqualToThreshold"
    statistic = "Sum"
    evaluation_periods = 1
    period = 60
    threshold = 2
    alarm_actions = [aws_sns_topic.extract_errors.arn]
    ok_actions = [aws_sns_topic.extract_errors.arn]
}
