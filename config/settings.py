import os

from dotenv import load_dotenv

load_dotenv() 


# OpenAQ source configuration
OPENAQ_BASE_URL = os.getenv( "OPENAQ_BASE_URL","https://api.openaq.org/v3")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")


# Azure account shared across our data lake layers
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

AZURE_RAW_FILE_SYSTEM = os.getenv("AZURE_RAW_FILE_SYSTEM", "raw")  # Immutable JSON source-of-truth container

AZURE_BRONZE_FILE_SYSTEM = os.getenv("AZURE_BRONZE_FILE_SYSTEM", "bronze")  # Structured Delta data produced from Raw

AZURE_SILVER_FILE_SYSTEM = os.getenv("AZURE_SILVER_FILE_SYSTEM","silver")  # Cleaned, validated and deduplicated Delta data