resource "aws_lambda_function" "extract_lambda" {
  filename = "${local.extract_function_zip_path}"
  function_name = "extract_lambda"
  role = aws_iam_role.lambda_role.arn
  handler = "extract.extraction_lambda_handler"
  runtime = "python3.9"
  layers = [aws_lambda_layer_version.lambda_layer.arn]
  depends_on = [null_resource.extraction_zip]
}
