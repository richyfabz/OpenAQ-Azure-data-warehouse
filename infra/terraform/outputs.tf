output "resource_group_id" {
  description = "ID of the observed platform resource group"
  value       = data.azurerm_resource_group.platform.id
}

output "storage_account_id" {
  description = "ID of the observed ADLS Gen2 storage account"
  value       = data.azurerm_storage_account.platform.id
}

output "storage_account_primary_dfs_endpoint" {
  description = "DFS endpoint used by the ingestion RawWriter and state backends"
  value       = data.azurerm_storage_account.platform.primary_dfs_endpoint
}

output "databricks_workspace_url" {
  description = "URL of the observed Databricks workspace"
  value       = data.azurerm_databricks_workspace.platform.workspace_url
}
