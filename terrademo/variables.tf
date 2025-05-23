variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}
variable "project" {
  description = "Project"
  default     = "even-acumen-450403-v4"
}

variable "region" {
  description = "Region of the project"
  default     = "northamerica-northeast2"
}

variable "location" {
  description = "Project location"
  default     = "northamerica-northeast2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "even-acumen-450403-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}