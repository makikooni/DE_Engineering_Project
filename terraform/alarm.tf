#=#=#=#=#=#=#=#=#=#=#=#=# Alarm

# Extract

resource "aws_cloudwatch_metric_alarm" "extract_lambda_error_alarm" {
  alarm_name          = "${local.extract_lambda_name}_alarm"
  alarm_description   = "This alarm monitors the ${local.extract_lambda_name}"
  metric_name         = "Errors"
  threshold           = "1"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "30"
  namespace           = "AWS/Lambda"
  alarm_actions       = [aws_sns_topic.extract_notification.arn]
  dimensions = {
    FunctionName = local.extract_lambda_name
  }
}

# Transform

resource "aws_cloudwatch_metric_alarm" "transform_lambda_error_alarm" {
  alarm_name          = "${local.transform_lambda_name}_alarm"
  alarm_description   = "This alarm monitors the ${local.transform_lambda_name}"
  metric_name         = "Errors"
  threshold           = "1"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "30"
  namespace           = "AWS/Lambda"
  alarm_actions       = [aws_sns_topic.transform_notification.arn]
  dimensions = {
    FunctionName = local.transform_lambda_name
  }
}

# Load

resource "aws_cloudwatch_metric_alarm" "load_lambda_error_alarm" {
  alarm_name          = "${local.load_lambda_name}_alarm"
  alarm_description   = "This alarm monitors the ${local.load_lambda_name}"
  metric_name         = "Errors"
  threshold           = "1"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "30"
  namespace           = "AWS/Lambda"
  alarm_actions       = [aws_sns_topic.load_notification.arn]
  dimensions = {
    FunctionName = local.load_lambda_name
  }
}
