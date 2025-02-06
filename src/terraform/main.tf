provider "google" {
	project = "my-gcp-project"
	region  = "us-central1"
}

resource "google_storage_bucket" "example" {
	name     = "my-example-bucket"
	location = "US"
 }
