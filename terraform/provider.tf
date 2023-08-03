provider "aws" {
  region = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket = "terraform-state-${local.suffix}"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}