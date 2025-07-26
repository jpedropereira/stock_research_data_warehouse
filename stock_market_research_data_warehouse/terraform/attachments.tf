# Attach policies to users
resource "minio_iam_user_policy_attachment" "airflow_attachment" {
  user_name   = minio_iam_user.airflow_user.name
  policy_name = minio_iam_policy.airflow_policy.id
}

resource "minio_iam_user_policy_attachment" "analytics_attachment" {
  user_name   = minio_iam_user.analytics_user.name
  policy_name = minio_iam_policy.analytics_policy.id
}

resource "minio_iam_user_policy_attachment" "readonly_attachment" {
  user_name   = minio_iam_user.readonly_user.name
  policy_name = minio_iam_policy.readonly_policy.id
}
