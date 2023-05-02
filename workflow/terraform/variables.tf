locals {
  data_lake_bucket = "nyc_bus_data"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "nyc_bus_dataset"
}

variable "BQ_DATASET_DBT_DEV" {
  description = "BigQuery Dataset used for dbt development"
  type = string
  default = "nyc_bus_dbt_dev"
}

variable "BQ_DATASET_DBT_PROD" {
  description = "BigQuery Dataset used for dbt production"
  type = string
  default = "nyc_bus_dbt_prod"
}
