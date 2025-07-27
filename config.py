import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

LLM_CONFIG = {
    "model": "gpt-4o-mini",
    "temperature": 0.3,
    "max_tokens": 1000
}

# BigQuery 설정 - keyfile.json만 사용
BIGQUERY_CONFIG = {
    "keyfile_path": "keyfile.json",  # 고정 경로
    "default_dataset": os.getenv("BIGQUERY_DEFAULT_DATASET", ""),
    "target_tables": os.getenv("BIGQUERY_TARGET_TABLES", "").split(",") if os.getenv("BIGQUERY_TARGET_TABLES") else []
}