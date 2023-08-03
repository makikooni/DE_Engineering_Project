variable "extract_lambda_name" {
  type    = string
  default = "extract_lambda"
}

variable "transform_lambda_name" {
  type    = string
  default = "transform_lambda"
}

variable "load_lambda_name" {
  type    = string
  default = "load_lambda"
}

variable "suffix" {
  type    = string
  default = "va-052023"
}

variable "db_secrets_arn" {
  type    = string
  default = "arn:aws:secretsmanager:eu-west-2:454963742860:secret:ingestion/db/credentials-y9n2MW"
}

variable "table_names_secrets_arn" {
  type    = string
  default = "arn:aws:secretsmanager:eu-west-2:454963742860:secret:ingestion/db/table-names-0OOOHO"
}