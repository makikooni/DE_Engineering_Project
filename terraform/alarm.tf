#=#=#=#=#=#=#=#=#=#=#=#=# Metric Filter

# Extract
resource "aws_cloudwatch_log_metric_filter" "extract_lambda_error_filter" {
    name = "${local.extract_lambda_name}_error_filter"
    log_group_name = "/aws/lambda/${local.extract_lambda_name}"
    pattern = "ERROR"

    metric_transformation {
        name = "ExtractLambdaErrorCount"
        namespace = local.metric_namespace
        value = "1"
    }
}

# Transform
resource "aws_cloudwatch_log_metric_filter" "transform_lambda_error_filter" {
    name = "${local.transform_lambda_name}_error_filter"
    log_group_name = "/aws/lambda/${local.transform_lambda_name}"
    pattern = "ERROR"

    metric_transformation {
        name = "TransformLambdaErrorCount"
        namespace = local.metric_namespace
        value = "1"
    }
}

# Load
resource "aws_cloudwatch_log_metric_filter" "load_lambda_error_filter" {
    name = "${local.load_lambda_name}_error_filter"
    log_group_name = "/aws/lambda/${local.load_lambda_name}"
    pattern = "ERROR"

    metric_transformation {
        name = "LoadLambdaErrorCount"
        namespace = local.metric_namespace
        value = "1"
    }
}


#=#=#=#=#=#=#=#=#=#=#=#=# Alarm

# Extract

resource "aws_cloudwatch_metric_alarm" "extract_lambda_error_alarm" {
  alarm_name          = "${local.extract_lambda_name}_alarm"
  alarm_description   = "This alarm monitors the ${local.extract_lambda_name}"
  metric_name         = aws_cloudwatch_log_metric_filter.extract_lambda_error_filter.name
  threshold           = "0"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "30"
  namespace           = local.metric_namespace
  alarm_actions       = [aws_sns_topic.extract_notification.arn]
}

# Transform

resource "aws_cloudwatch_metric_alarm" "transform_lambda_error_alarm" {
  alarm_name          = "${local.transform_lambda_name}_alarm"
  alarm_description   = "This alarm monitors the ${local.transform_lambda_name}"
  metric_name         = aws_cloudwatch_log_metric_filter.transform_lambda_error_filter.name
  threshold           = "0"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "30"
  namespace           = local.metric_namespace
  alarm_actions       = [aws_sns_topic.transform_notification.arn]
}

# Load

resource "aws_cloudwatch_metric_alarm" "load_lambda_error_alarm" {
  alarm_name          = "${local.load_lambda_name}_alarm"
  alarm_description   = "This alarm monitors the ${local.load_lambda_name}"
  metric_name         = aws_cloudwatch_log_metric_filter.load_lambda_error_filter.name
  threshold           = "0"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "30"
  namespace           = local.metric_namespace
  alarm_actions       = [aws_sns_topic.load_notification.arn]
}


# resource "aws_cloudwatch_event_rule" "scheduler" {
#     name_prefix = "extraction-scheduler-"
#     schedule_expression = "rate(3 minutes)"
# }

# resource "aws_lambda_permission" "allow_scheduler" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.extract_lambda.function_name
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.scheduler.arn
#   source_account = data.aws_caller_identity.current.account_id
# }

# resource "aws_cloudwatch_event_target" "lambda_target" {
#   rule      = aws_cloudwatch_event_rule.scheduler.name
#   arn       = aws_lambda_function.extract_lambda.arn
# }
