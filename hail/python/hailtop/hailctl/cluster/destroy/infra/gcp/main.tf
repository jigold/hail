terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.48.0"
    }
  }

  backend "gcs" {
#    bucket  = # Set with -backend-config "bucket=BUCKET"
    prefix  = "mini-batch/terraform/driver/state"
  }
}
