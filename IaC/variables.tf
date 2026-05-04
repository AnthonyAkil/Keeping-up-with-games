variable "project" {
  description = "Short project name used in all resource names. Ensure it will be globally unique."
  type        = string
}

variable "location" {
  description = "Azure region to deploy into, e.g. westeurope."
  type        = string
  default     = "westeurope"
}