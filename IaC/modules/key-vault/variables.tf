variable "name" {
  description = "Key Vault name — globally unique, 3-24 chars, letters, numbers and hyphens only."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group to deploy into."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}