data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "extraction_lambda_zip" {
  type        = "zip"
  excludes = [
    "${path.module}/../src/.github",
    "${path.module}/../src/tests",
    "${path.module}/../src/.gitignore",
    "${path.module}/../src/lambda_layer_requirements.txt",
    "${path.module}/../src/Makefile",
    "${path.module}/../src/README.md",
    "${path.module}/../src/requirements.txt",
    "${path.module}/../src/load.py",
    "${path.module}/../src/transform.py"
  ]
  source_dir = "${path.module}/.."
  output_path = "${path.module}/../extraction_function.zip"
}