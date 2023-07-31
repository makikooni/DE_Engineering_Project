data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = [
    "${path.module}/../src/extract/extract.py",
    "${path.module}/../src/utils"
    ]
  output_path = "${path.module}/../function.zip"
}
