terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.3.7"
  backend "local" {}
}

provider "aws" {
  region  = var.region #"us-east-1"
  profile = var.profile #"service_wp"
}

# s3 bucket for data lake
# weather-data-kpde
resource "aws_s3_bucket" "bucket_data_lake" {
  bucket        = var.bucket #"weather-data-kpde"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "public_access_data_lake" {
  bucket = aws_s3_bucket.bucket_data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption_data_lake" {
  bucket = aws_s3_bucket.bucket_data_lake.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_data_lake" {
  bucket = aws_s3_bucket.bucket_data_lake.id

  rule {
    id = "lifecycle-global"

    filter {}

    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    expiration {
      days = 120
    }
  }
}


# redshift instance for data warehouse
resource "aws_redshift_cluster" "redshift_warehouse" {
  cluster_identifier  = "redshift-kpde"
  database_name       = "dev"
  master_username     = "service_wp"
  master_password     = "serviceWp1"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  publicly_accessible = true
  iam_roles           = ["arn:aws:iam::655268872845:role/redshift-cluster-role"]
}