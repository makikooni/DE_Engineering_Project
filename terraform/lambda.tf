resource "aws_lambda_function" "extract_lambda" {
  filename = "${local.extract_function_zip_path}"
  function_name = "${local.extract_lambda_name}"
  role = aws_iam_role.lambda_role.arn
  handler = "extract.extraction_lambda_handler"
  runtime = "python3.9"
  layers = [aws_lambda_layer_version.lambda_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:8"]
  timeout = 60
  depends_on = [null_resource.extraction_zip]
}
