variable "name" {
  type = string
}

variable "iam_roles" {
  type = list(string)
  default = []
}

variable "gcp_project" {
  type = string
}
