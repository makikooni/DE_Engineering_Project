locals {
  extract_lambda_name     =   "extract_lambda"
  transform_lambda_name   =   "transform_lambda"
  load_lambda_name        =   "load_lambda"
  suffix                  =   "va-052023"
  db_secrets_arn          =   "arn:aws:secretsmanager:eu-west-2:454963742860:secret:ingestion/db/credentials-y9n2MW"
  table_names_secrets_arn =   "arn:aws:secretsmanager:eu-west-2:454963742860:secret:ingestion/db/table-names-0OOOHO"
  group_name              =   "variousartists"
}