data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
locals {
  temp_extract_lambda_dir= "${path.root}/../temp_extract_lambda"
  extract_function_zip_path = "${path.module}/../extraction_function.zip"
  extract_function_path = "${path.root}/../src/extract.py"
  utils_path = "${path.root}/../utils"
}

resource "null_resource" "extraction_zip" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir ${local.temp_extract_lambda_dir}
      cp -r ${local.extract_function_path} ${local.utils_path} ${local.temp_extract_lambda_dir}
      cd ${local.temp_extract_lambda_dir} && zip -r ${local.extract_function_zip_path} ${local.temp_extract_lambda_dir}
    EOT
  }
}


# data "archive_file" "extraction_lambda_zip" {
#   type        = "zip"
#   excludes = [
#     "${path.module}/../.github",
#     "${path.module}/../src/load.py",
#     "${path.module}/../src/transform.py",
#     "${path.module}/../terraform",
#     "${path.module}/../tests",
#     "${path.module}/../.gitignore",
#     "${path.module}/../lambda_layer_requirements.txt",
#     "${path.module}/../Makefile",
#     "${path.module}/../README.md",
#     "${path.module}/../requirements.txt"
#   ]
#   source_dir = "${path.module}/.."
#   output_path = "${path.module}/../extraction_function.zip"
# }