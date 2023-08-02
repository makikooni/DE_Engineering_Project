data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "null_resource" "extraction_zip" {
  provisioner "local-exec" {
    command = "zip -r extraction_function.zip ${path.root}/../src/extract.py ${path.root}/../utils"
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