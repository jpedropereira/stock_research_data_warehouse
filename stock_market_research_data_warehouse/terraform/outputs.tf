output "bucket_names" {
  description = "Names of all created buckets"
  value = {
    data_sources = [for bucket in minio_s3_bucket.data_source_buckets : bucket.bucket]
    archive      = minio_s3_bucket.archive_bucket.bucket
  }
}

output "user_credentials" {
  description = "User access credentials (sensitive)"
  sensitive   = true
  value = {
    airflow = {
      access_key = minio_iam_service_account.airflow_service_account.access_key
      secret_key = minio_iam_service_account.airflow_service_account.secret_key
    }
    analytics = {
      access_key = minio_iam_service_account.analytics_service_account.access_key
      secret_key = minio_iam_service_account.analytics_service_account.secret_key
    }
    readonly = {
      access_key = minio_iam_service_account.readonly_service_account.access_key
      secret_key = minio_iam_service_account.readonly_service_account.secret_key
    }
  }
}

output "minio_endpoints" {
  description = "Minio service endpoints"
  value = {
    api_endpoint     = var.minio_server
    console_endpoint = replace(var.minio_server, ":9000", ":9001")
  }
}
