#!/bin/bash
# Quick setup script for Terraform Minio management

set -e

echo "🏗️ Setting up Terraform for Minio management..."

# Check if Docker services are running
echo "📦 Checking if Minio is running..."
if ! curl -f http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "❌ Minio is not running. Please start it first:"
    echo "   astro dev start"
    exit 1
fi

echo "✅ Minio is running!"

# Navigate to terraform directory
cd terraform

# Copy variables file if it doesn't exist
if [ ! -f "terraform.tfvars" ]; then
    echo "📝 Creating terraform.tfvars from example..."
    cp terraform.tfvars.example terraform.tfvars
    echo "⚠️  Please edit terraform.tfvars with your credentials before continuing."
    exit 0
fi

# Initialize Terraform
echo "🔧 Initializing Terraform..."
terraform init

# Plan deployment
echo "📋 Planning Terraform deployment..."
terraform plan

echo ""
echo "🚀 Ready to apply! Run:"
echo "   cd terraform && terraform apply"
echo ""
echo "📊 After applying, view results with:"
echo "   terraform output bucket_names"
echo "   terraform output minio_endpoints"
