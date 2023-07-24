resource "aws_s3_bucket" "ingestion_zone" {
  bucket = "ingestion-VA-052023"
}

resource "aws_s3_bucket" "processed_zone" {
  bucket = "processed-VA-052023"
}