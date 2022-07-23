
locals {
  data_lake_bucket = "movielens_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for the bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "movielens_25m"
}