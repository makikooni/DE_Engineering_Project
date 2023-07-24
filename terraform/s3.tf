resource "aws_s3_bucket" "ingestion_zone" {
  bucket = "ingestion-va-052023"
}

resource "aws_s3_bucket" "processed_zone" {
  bucket = "processed-va-052023"
}