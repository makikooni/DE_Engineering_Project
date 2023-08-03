resource "aws_s3_bucket" "ingestion_zone" {
  bucket = "ingestion-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "processed_zone" {
  bucket = "processed-${local.suffix}"
  force_destroy = true
}
