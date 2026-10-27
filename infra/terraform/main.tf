terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Local backend for now; remote state is a later-phase decision.
}

provider "azurerm" {
  features {}
}

# --- Observed, not managed -------------------------------------------------
# These resources already exist (created manually during Phases 1-2) and are
# referenced read-only so later phases can wire new resources to them without
# hardcoding IDs. Import blocks that would bring them under Terraform
# management are intentionally deferred — see README.md.

data "azurerm_resource_group" "platform" {
  name = var.resource_group_name
}

data "azurerm_storage_account" "platform" {
  name                = var.storage_account_name
  resource_group_name = data.azurerm_resource_group.platform.name
}

data "azurerm_databricks_workspace" "platform" {
  name                = var.databricks_workspace_name
  resource_group_name = data.azurerm_resource_group.platform.name
}

# Deferred imports (DO NOT uncomment without explicit approval — see README.md):
#
# import {
#   to = azurerm_resource_group.platform
#   id = "/subscriptions/${var.subscription_id}/resourceGroups/${var.resource_group_name}"
# }
#
# import {
#   to = azurerm_storage_account.platform
#   id = "${data.azurerm_resource_group.platform.id}/providers/Microsoft.Storage/storageAccounts/${var.storage_account_name}"
# }
