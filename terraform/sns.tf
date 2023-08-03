#=#=#=#=#=#=#=#=#=#=#=#=# Simple Notification System

# Extract

resource "aws_sns_topic" "extract_notification" {
    name = "${local.extract_lambda_name}_notification"
}

resource "aws_sns_topic_subscription" "extract_notification_target" {
    topic_arn = aws_sns_topic.extract_notification.arn
    protocol = "email"
    endpoint = "atif.hussain.khan@hotmail.com"
}

# Transform 

# resource "aws_sns_topic" "transform_notification" {
#     name = "${local.transform_lambda_name}_notification"
# }

# resource "aws_sns_topic_subscription" "transform_notification_target" {
#     topic_arn = aws_sns_topic.transform_notification.arn
#     protocol = "email"
#     endpoint = "atif.hussain.khan@hotmail.com"
# }

# Load 

# resource "aws_sns_topic" "load_notification" {
#     name = "${local.load_lambda_name}_notification"
# }

# resource "aws_sns_topic_subscription" "load_notification_target" {
#     topic_arn = aws_sns_topic.load_notification.arn
#     protocol = "email"
#     endpoint = "atif.hussain.khan@hotmail.com"
# }