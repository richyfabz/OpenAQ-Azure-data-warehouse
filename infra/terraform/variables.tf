variable "subscription_id" {
  description = "Azure subscription hosting the OpenAQ data platform"
  type        = string
}

variable "resource_group_name" {
  description = "Existing resource group for the OpenAQ data platform"
  type        = string
  default     = "rg-openaq-data-platform"
}

variable "storage_account_name" {
  description = "Existing ADLS Gen2 storage account backing the Raw volume"
  type        = string
  default     = "openaqdataplatform"
}

variable "databricks_workspace_name" {
  description = "Existing Databricks workspace (Free Edition)"
  type        = string
  default     = "openaq-databricks"
}

variable "location" {
  description = "Azure region for any future managed resources"
  type        = string
  default     = "southafricanorth"
}
