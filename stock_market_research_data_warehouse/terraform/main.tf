terraform {
  required_version = ">= 1.0"

  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "~> 2.5"
    }
  }
}

provider "minio" {
  minio_server   = var.minio_server
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = var.minio_ssl
}
