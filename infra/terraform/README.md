# Terraform — OpenAQ data platform (Phase 4: observe-only skeleton)

## What exists in Azure today (inventoried 2026-10-25 via `az`)

Subscription `baf36fbb-ddcc-4e23-99da-c45f19d6d69b`, region `southafricanorth`:

| Resource | Type | Notes |
| --- | --- | --- |
| `rg-openaq-data-platform` | Resource group | Platform RG, status Succeeded |
| `openaqdataplatform` | Storage account (StorageV2, HNS enabled) | ADLS Gen2; containers `raw`, `bronze`, `silver`; backs the UC volume `/Volumes/workspace/raw/openaq_landing` |
| `openaq-databricks` | Databricks workspace | Free Edition, `adb-7405608152735347.7.azuredatabricks.net` |
| `databricks-rg-openaq-databricks-k5eekomzqleda` | Resource group | Databricks-managed RG (contains `dbstoragessc4xgluuumtg`); never manage directly |
| `NetworkWatcherRG` | Resource group | Azure-managed; ignore |

## Manage vs. observe

This phase **observes** everything: the resource group, storage account, and
Databricks workspace are declared as `data` sources only. Terraform manages
nothing yet — `terraform plan` is expected to report zero changes.

## Why imports are deferred

Importing the existing resources would make Terraform authoritative for their
full attribute surface. Any mismatch between the real configuration and our
`.tf` declarations (retention policies, network rules, Databricks-managed
settings we have not modelled) would then show up as drift — or worse, a
future `apply` could force recreation of the storage account that backs the
Raw volume. Until each resource's live configuration has been fully captured
in HCL and reviewed, imports stay commented out in `main.tf`. Enabling them is
a later-phase decision requiring explicit approval.

## Usage

```bash
cd infra/terraform
terraform init
terraform fmt -check
terraform validate
terraform plan -var-file=environments/dev.tfvars   # expect: no changes
```

Authentication uses the ambient `az login` session (azurerm provider default).
Never run `terraform apply` in this phase.
