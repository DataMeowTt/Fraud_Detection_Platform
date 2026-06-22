from minio import Minio
from pathlib import Path

model_path = Path("data/fraud_model.json")

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False,
)

client.fput_object(
    bucket_name="ml-models",
    object_name="fraud_model.json",
    file_path=str(model_path),
    content_type="application/json",
)

print(f"Uploaded: {model_path.name} → ml-models/fraud_model.json")
