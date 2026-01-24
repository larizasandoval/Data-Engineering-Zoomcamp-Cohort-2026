terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

 # Connect to gcp using ADC (identity verification)
 provider "google" {
   project = var.project
   region  = var.region
   zone    = var.zone
 }

 # add these data blocks 
 
 # This data source gets a temporary token for the service account
 data "google_service_account_access_token" "default" {
   provider               = google
   target_service_account = "terraform-runner@arctic-eye-479622-i5.iam.gserviceaccount.com"
   scopes                 = ["https://www.googleapis.com/auth/cloud-platform"]
   lifetime               = "3600s"
 }
 
 # This second provider block uses that temporary token and does the real work
 provider "google" {
   alias        = "impersonated"
   access_token = data.google_service_account_access_token.default.access_token
   project      = var.project
   region       = var.region
   zone         = var.zone
 }