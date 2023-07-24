resource "aws_s3_bucket" "ingestion_zone" {
  bucket_name = "ingestion-VA-052023"
}

resource "aws_s3_bucket" "processed_zone" {
  bucket_name = "processed-VA-052023"
}