resource "aws_lambda_function" "extract_lambda" {
  filename = "${local.extract_function_zip_path}"
  function_name = "${local.extract_lambda_name}"
  role = aws_iam_role.extract_lambda_role.arn
  handler = "extract.extraction_lambda_handler"
  runtime = "python3.9"
  layers = [aws_lambda_layer_version.lambda_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:8"]
  timeout = 60  # 1 minute
  depends_on = [null_resource.extraction_zip]
}

resource "aws_lambda_function" "transform_lambda" {
  filename = "${local.transform_function_zip_path}"
  function_name = "${local.transform_lambda_name}"
  role = aws_iam_role.transform_lambda_role.arn
  handler = "transform.transform_lambda_handler"
  runtime = "python3.9"
  layers = [aws_lambda_layer_version.lambda_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:8"]
  timeout = 300 # 5 minutes
  depends_on = [null_resource.transform_zip]
}

resource "aws_lambda_function" "load_lambda" {
  filename = "${local.load_function_zip_path}"
  function_name = "${local.load_lambda_name}"
  role = aws_iam_role.load_lambda_role.arn
  handler = "load.load_lambda_handler"
  runtime = "python3.9"
  layers = [aws_lambda_layer_version.lambda_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:8"]
  timeout = 60  # 1 minute
  depends_on = [null_resource.load_zip]
}