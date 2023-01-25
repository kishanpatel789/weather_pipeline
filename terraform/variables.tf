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