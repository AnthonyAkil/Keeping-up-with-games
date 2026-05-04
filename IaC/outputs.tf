output "resource_group_name" {
  value = module.resource_group.name
}

output "storage_account_name" {
  value = module.storage_account.name
}

output "blob_container_name" {
  value = module.blob_container.name
}

output "key_vault_uri" {
  value = module.key_vault.vault_uri
}