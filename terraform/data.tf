data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


locals {
  source_files = ["${path.module}/../src/extract.py", "${path.module}/../utils"]
}

data "template_file" "t_file" {
  count = "${length(local.source_files)}"

  template = "${file(element(local.source_files, count.index))}"
}


data "archive_file" "archive" {
  type        = "zip"
  output_path = "${path.module}/../extraction_function.zip"

  source {
    filename = "${basename(local.source_files[0])}"
    content  = "${data.template_file.t_file.0.rendered}"
  }

  source {
    filename = "${basename(local.source_files[1])}"
    content  = "${data.template_file.t_file.1.rendered}"
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