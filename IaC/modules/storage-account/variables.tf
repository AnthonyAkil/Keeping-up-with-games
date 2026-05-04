variable "name" {
  description = "Storage account name — must be globally unique, 3-24 lowercase letters and numbers only, no hyphens."
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