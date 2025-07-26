# Data Source Buckets - Blueprint for future expansion
#
# Usage: Simply add new source names to the list below and run 'terraform apply'

resource "minio_s3_bucket" "data_source_buckets" {
  for_each = toset([
    "example-data-source"  # Replace with the first source
  ])

  bucket        = "${var.project_name}-${each.key}-${var.environment}"
  force_destroy = true
}

# Archive bucket for long-term storage
resource "minio_s3_bucket" "archive_bucket" {
  bucket        = "${var.project_name}-archive-${var.environment}"
  force_destroy = true
}
