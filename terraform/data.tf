#=#=#=#=#=#=#=#=#=#=#=#=# Data

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
locals {
  temp_extract_lambda_dir= "${path.root}/../temp_${local.extract_lambda_name}"
  temp_transform_lambda_dir= "${path.root}/../temp_${local.transform_lambda_name}"
  temp_load_lambda_dir= "${path.root}/../temp_${local.load_lambda_name}"
  
  extract_function_path = "${path.root}/../src/extract.py"
  transform_function_path = "${path.root}/../src/transform.py"
  load_function_path = "${path.root}/../src/load.py"

  extract_function_zip_path = "${path.module}/../extraction_function.zip"
  transform_function_zip_path = "${path.module}/../transform_function.zip"
  load_function_zip_path = "${path.module}/../load_function.zip"
  
  utils_path = "${path.root}/../utils"
}

# Extract

resource "null_resource" "extraction_zip" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir ${local.temp_extract_lambda_dir}
      cp -r ${local.extract_function_path} ${local.utils_path} ${local.temp_extract_lambda_dir}
      cd ${local.temp_extract_lambda_dir} 
      zip -r ${local.extract_function_zip_path} .
    EOT
  }
}

# Transform

resource "null_resource" "transform_zip" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir ${local.temp_transform_lambda_dir}
      cp -r ${local.transform_function_path} ${local.utils_path} ${local.temp_transform_lambda_dir}
      cd ${local.temp_transform_lambda_dir} 
      zip -r ${local.transform_function_zip_path} .
    EOT
  }
}

# Load

resource "null_resource" "load_zip" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir ${local.temp_load_lambda_dir}
      cp -r ${local.load_function_path} ${local.utils_path} ${local.temp_load_lambda_dir}
      cd ${local.temp_load_lambda_dir} 
      zip -r ${local.load_function_zip_path} .
    EOT
  }
}