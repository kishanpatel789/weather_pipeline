variable "region" {
    description = "Region for AWS resoures."
    default = "us-east-1"
    type = string
}

variable "profile" {
    description = "Profile to be utilized in local AWS CLI configuration"
    default = "service_wp"
    type = string
}

variable "bucket" {
    description = "Name of S3 bucket; must be globally unique"
    default = "weather-data-kpde"
    type = string
}

variable "redshift_cluster_name" {
    description = "Name of redshift cluster"
    default = "redshift-kpde"
    type = string
}

variable "redshift_cluster_role" {
    description = "ARN for role that has AmazonS3ReadOnlyAccess and AmazonRedshiftAllCommandsFullAccess policies"
    default = "arn:aws:iam::655268872845:role/redshift-cluster-role"
    type = string
}