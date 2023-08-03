resource "aws_s3_bucket" "ingestion_zone" {
  bucket = "ingestion-${var.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "processed_zone" {
  bucket = "processed-${var.suffix}"
  force_destroy = true
}
