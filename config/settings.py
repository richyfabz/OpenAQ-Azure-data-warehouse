import os

from dotenv import load_dotenv

load_dotenv() 


# OpenAQ source configuration
OPENAQ_BASE_URL = os.getenv( "OPENAQ_BASE_URL","https://api.openaq.org/v3")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
OPENAQ_RAW_VOLUME_ROOT = os.getenv(
    "OPENAQ_RAW_VOLUME_ROOT",
    "/Volumes/workspace/raw/openaq_landing",
)
OPENAQ_LANDING_MODE = os.getenv("OPENAQ_LANDING_MODE", "local").strip().lower()


# Azure account shared across our data lake layers
# Deprecated for Raw writes. Kept for the state backend and backward compatibility.
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

# Deprecated for Raw writes. The Raw source of truth is the UC volume above.
AZURE_RAW_FILE_SYSTEM = os.getenv("AZURE_RAW_FILE_SYSTEM", "raw")

AZURE_BRONZE_FILE_SYSTEM = os.getenv("AZURE_BRONZE_FILE_SYSTEM", "bronze")  # Structured Delta data produced from Raw

AZURE_SILVER_FILE_SYSTEM = os.getenv("AZURE_SILVER_FILE_SYSTEM","silver")  # Cleaned, validated and deduplicated Delta data
