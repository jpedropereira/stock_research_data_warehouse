# Airflow Policy - Full access to all data source buckets
resource "minio_iam_policy" "airflow_policy" {
  name = "${var.project_name}-airflow-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          # All data source buckets - flexible pattern for any future buckets
          "arn:aws:s3:::${var.project_name}-*-${var.environment}",
          "arn:aws:s3:::${var.project_name}-*-${var.environment}/*"
        ]
      }
    ]
  })
}

# Analytics Policy - Read-only access to all data source buckets
resource "minio_iam_policy" "analytics_policy" {
  name = "${var.project_name}-analytics-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          # Read access to all data source buckets (flexible for future expansion)
          "arn:aws:s3:::${var.project_name}-*-${var.environment}",
          "arn:aws:s3:::${var.project_name}-*-${var.environment}/*"
        ]
      }
    ]
  })
}

# Read-only Policy - Read access to all data source buckets
resource "minio_iam_policy" "readonly_policy" {
  name = "${var.project_name}-readonly-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          # Read access to all data source buckets
          "arn:aws:s3:::${var.project_name}-*-${var.environment}",
          "arn:aws:s3:::${var.project_name}-*-${var.environment}/*"
        ]
      }
    ]
  })
}
