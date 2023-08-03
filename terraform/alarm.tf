#=#=#=#=#=#=#=#=#=#=#=#=# Metric Filter

# Extract
# resource "aws_cloudwatch_log_metric_filter" "extract_lambda_error_filter" {
#     name = "${local.extract_lambda_name}_error_filter"
#     log_group_name = "/aws/lambda/${local.extract_lambda_name}"
#     pattern = "ERROR"

#     metric_transformation {
#         name = "ExtractLambdaErrorCount"
#         namespace = local.metric_namespace
#         value = "1"
#     }
# }

# Transform
# resource "aws_cloudwatch_log_metric_filter" "transform_lambda_error_filter" {
#     name = "${local.transform_lambda_name}_error_filter"
#     log_group_name = "/aws/lambda/${local.transform_lambda_name}"
#     pattern = "ERROR"

#     metric_transformation {
#         name = "TransformLambdaErrorCount"
#         namespace = local.metric_namespace
#         value = "1"
#     }
# }

# Load
# resource "aws_cloudwatch_log_metric_filter" "load_lambda_error_filter" {
#     name = "${local.load_lambda_name}_error_filter"
#     log_group_name = "/aws/lambda/${local.load_lambda_name}"
#     pattern = "ERROR"

#     metric_transformation {
#         name = "LoadLambdaErrorCount"
#         namespace = local.metric_namespace
#         value = "1"
#     }
# }


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
  period              = "180"
  namespace           = "AWS/Lambda"
  alarm_actions       = [aws_sns_topic.extract_notification.arn]
  dimensions = {
    FunctionName = local.extract_lambda_name
  }
}

# Transform

# resource "aws_cloudwatch_metric_alarm" "transform_lambda_error_alarm" {
#   alarm_name          = "${local.transform_lambda_name}_alarm"
#   alarm_description   = "This alarm monitors the ${local.transform_lambda_name}"
#   metric_name         = aws_cloudwatch_log_metric_filter.transform_lambda_error_filter.name
#   threshold           = "0"
#   statistic           = "Sum"
#   comparison_operator = "GreaterThanThreshold"
#   datapoints_to_alarm = "1"
#   evaluation_periods  = "1"
#   period              = "30"
#   namespace           = local.metric_namespace
#   alarm_actions       = [aws_sns_topic.transform_notification.arn]
# }

# Load

# resource "aws_cloudwatch_metric_alarm" "load_lambda_error_alarm" {
#   alarm_name          = "${local.load_lambda_name}_alarm"
#   alarm_description   = "This alarm monitors the ${local.load_lambda_name}"
#   metric_name         = aws_cloudwatch_log_metric_filter.load_lambda_error_filter.name
#   threshold           = "0"
#   statistic           = "Sum"
#   comparison_operator = "GreaterThanThreshold"
#   datapoints_to_alarm = "1"
#   evaluation_periods  = "1"
#   period              = "30"
#   namespace           = local.metric_namespace
#   alarm_actions       = [aws_sns_topic.load_notification.arn]
# }
