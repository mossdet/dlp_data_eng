variable credentials {
  description = "Service account credentials"
  default = "./keys/my-creds.json"
}

variable project_id {
  description = "Project ID"
    default     = "terraform-demo-449904"
}

variable region {
  description = "Project region"
  default     = "us-west3"
}

variable "location" {
  description = "Project location"
  default     = "us-west3"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "terraform-demo-449904-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage Class"
  default     = "STANDARD"
}