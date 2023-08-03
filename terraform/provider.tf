provider "aws" {
  region = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket = "terraform-state-${var.suffix}"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}