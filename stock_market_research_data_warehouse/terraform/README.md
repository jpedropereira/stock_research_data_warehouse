# Terraform Minio Management

This directory contains Terraform configuration to manage Minio buckets, users, and policies for the Stock Research Data Warehouse project.

## 🏗️ Architecture

- **Container Management**: Docker Compose (`docker-compose.override.yml`)
- **Configuration Management**: Terraform (this directory)

## 📦 What Terraform Manages

### Buckets
- **Data Sources**: One bucket per data source (e.g., `stock-research-dw-example-data-source-dev`)
- **Archive**: Long-term storage (`stock-research-dw-archive-dev`)

### Users & Permissions
- **airflow**: Full access to all data source buckets for data ingestion
- **analytics**: Read-only access to all data source buckets for analysis
- **readonly**: Read-only access for monitoring and reporting

## 🚀 Getting Started

### Prerequisites
1. **Start Minio container** first:
   ```bash
   cd ..
   astro dev start
   ```

2. **Install Terraform locally**:

   Consult documentation official documentation at:
   https://developer.hashicorp.com/terraform/install

   You can confirm it is sucessfully installed by running:
   ``` bash
   terraform --version
   ```

### Setup

1. **Copy variables file**:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

2. **Update credentials** in `terraform.tfvars` to match your `.env.local`:
   ```hcl
   minio_root_user     = "your-username"     # From MINIO_ROOT_USER
   minio_root_password = "your-password"     # From MINIO_ROOT_PASSWORD
   ```


3. **Initialize Terraform**:
   ```bash
   terraform init
   ```

4. **Plan the deployment**:
   ```bash
   terraform plan
   ```

5. **Apply configuration**:
   ```bash
   terraform apply
   ```

## 📊 Viewing Results

After applying:

```bash
# See created buckets
terraform output bucket_names

# Get user credentials (sensitive)
terraform output -json user_credentials

# See service endpoints
terraform output minio_endpoints
```

## 🔧 Managing Changes

### Add New Data Source
Edit `buckets.tf` and add to the data source list:

```hcl
resource "minio_s3_bucket" "data_source_buckets" {
  for_each = toset([
    "example-data-source",
    "stock-prices",        # Add your new data source here
    "financial-reports"    # Add another one
  ])
  # ...
}
```

Then run `terraform apply` to create the new buckets.

### Update Policies
Edit `policies.tf` to modify user permissions.

### Different Environments
You can deploy the same configuration to multiple environments by overriding variables at runtime:

```bash
# Staging
terraform apply -var="environment=staging" -var="minio_server=staging.minio.com:9000"

# Production
terraform apply -var="environment=prod" -var="minio_server=prod.minio.com:9000"
```

## 🧹 Cleanup

To destroy all Terraform-managed resources:

```bash
terraform destroy
```

**Note**: This will delete all buckets and data! The Docker container will remain running.

## 📁 File Structure

- `main.tf` - Provider configuration
- `variables.tf` - Variable definitions
- `buckets.tf` - S3 bucket resources
- `users.tf` - IAM users and access keys
- `policies.tf` - IAM policies
- `attachments.tf` - Policy-user attachments
- `outputs.tf` - Output values
- `terraform.tfvars.example` - Example variables file

## 🔄 Integration with Airflow

After applying Terraform, you can use the generated credentials in your Airflow connections:

1. Get credentials: `terraform output -json user_credentials`
2. Update the Airflow S3 connection in using `airflow_settings.yaml` with the `airflow` user credentials
3. Use bucket names from: `terraform output bucket_names`

### Recommended Folder Structure in Buckets
```
stock-research-dw-your-data-source-dev/
├── 2025/01/26/
│   ├── data_20250126_090000.json
│   └── data_20250126_120000.json
├── 2025/01/27/
│   └── data_20250127_090000.json
└── 2025/02/01/
    └── ...
```
