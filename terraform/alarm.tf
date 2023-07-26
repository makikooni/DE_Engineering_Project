resource "aws_sns_topic" "extract_errors" {
    name = "extract-errors-topic"
}

resource "aws_sns_topic_subscription" "extract_errors_email_target" {
    topic_arn = aws_sns_topic.extract_errors.arn
    protocol = "email"
    endpoint = "david.geddes.de-202307@northcoders.net"
}







