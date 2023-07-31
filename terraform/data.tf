data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "extraction_lambda_zip" {
  type        = "zip"
  excludes = [
    "${path.module}/../src/load.py",
    "${path.module}/../src/transform.py"
  ]
  source_dir = "${path.module}/../src"
  output_path = "${path.module}/../extraction_function.zip"
}