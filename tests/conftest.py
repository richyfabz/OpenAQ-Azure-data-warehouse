import os

# CI has no .env — tests must not depend on local developer environment.
os.environ.setdefault("OPENAQ_API_KEY", "test-placeholder")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "testplaceholder")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_KEY", "dGVzdC1wbGFjZWhvbGRlcg==")  # if required