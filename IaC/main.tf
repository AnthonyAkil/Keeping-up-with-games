terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

module "resource_group" {
  source   = "./modules/resource-group"
  name     = "${var.project}-rg"
  location = var.location
}

module "storage_account" {
  source              = "./modules/storage-account"
  name                = "${var.project}sa"
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
}

module "blob_container" {
  source               = "./modules/blob-container"
  name                 = "${var.project}-container"
  storage_account_name = module.storage_account.name
}

module "key_vault" {
  source              = "./modules/key-vault"
  name                = "${var.project}-kv"
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
}