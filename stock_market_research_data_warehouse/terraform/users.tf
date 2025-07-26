# Airflow Service User
resource "minio_iam_user" "airflow_user" {
  name          = "${var.project_name}-airflow-${var.environment}"
  force_destroy = true
  tags = {
    Environment = var.environment
    Project     = var.project_name
    Service     = "airflow"
  }
}

resource "minio_iam_service_account" "airflow_service_account" {
  target_user = minio_iam_user.airflow_user.name
}

# Analytics Service User (for notebooks, ML models, etc.)
resource "minio_iam_user" "analytics_user" {
  name          = "${var.project_name}-analytics-${var.environment}"
  force_destroy = true
  tags = {
    Environment = var.environment
    Project     = var.project_name
    Service     = "analytics"
  }
}

resource "minio_iam_service_account" "analytics_service_account" {
  target_user = minio_iam_user.analytics_user.name
}

# Read-only user for monitoring/reporting
resource "minio_iam_user" "readonly_user" {
  name          = "${var.project_name}-readonly-${var.environment}"
  force_destroy = true
  tags = {
    Environment = var.environment
    Project     = var.project_name
    Service     = "monitoring"
  }
}

resource "minio_iam_service_account" "readonly_service_account" {
  target_user = minio_iam_user.readonly_user.name
}
