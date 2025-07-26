variable "minio_server" {
  description = "Minio server endpoint"
  type        = string
  default     = "localhost:9000"
}

variable "minio_root_user" {
  description = "Minio root username"
  type        = string
  sensitive   = true
}

variable "minio_root_password" {
  description = "Minio root password"
  type        = string
  sensitive   = true
}

variable "minio_ssl" {
  description = "Use SSL for Minio connection"
  type        = bool
  default     = false
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "stock-research-dw"
}
