from datetime import datetime
import re
import os
import tempfile
import shutil
import pandas as pd
from qdrant_client import QdrantClient
import boto3


def export_collection_to_parquet(client, collection_name, output_file, batch_size=1000):
    """Scroll through a collection and save points into a Parquet file"""
    all_points = []
    next_page = None

    while True:
        points, next_page = client.scroll(
            collection_name=collection_name,
            with_vectors=True,
            with_payload=True,
            limit=batch_size,
            offset=next_page,
        )
        if not points:
            break

        for p in points:
            all_points.append({
                "id": p.id,
                "vector": p.vector,
                "payload": p.payload
            })

        if next_page is None:
            break

    if all_points:
        df = pd.DataFrame(all_points)
        
        df.to_parquet(output_file, index=False, compression="zstd")
        print(f"Saved {len(df)} points from {collection_name} to {output_file}")
        return True
    else:
        print(f"No data found in {collection_name}")
        return False


def clean_and_archive_collections():
    """
    Archive old Qdrant collections into MinIO.
    Store raw Parquet in folders inside bucket by prefix name.
    """
    # prod_<prefix>_<YYYYMMDD>
    PATTERN = re.compile(r"^prod_([a-zA-Z0-9]+)_(\d{8})$")
    KEEP_DAYS = 3

    # Qdrant client
    client = QdrantClient(host="127.0.0.1", port=6333, timeout=120.0)

    # MinIO client
    minio_endpoint = "http://127.0.0.1:9000"
    minio_bucket = "qdrant-archives"
    minio_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    # Ensure bucket exists
    try:
        minio_client.head_bucket(Bucket=minio_bucket)
    except Exception:
        minio_client.create_bucket(Bucket=minio_bucket)

    collections = client.get_collections().collections
    today = datetime.today()

    for col in collections:
        match = PATTERN.match(col.name)
        if not match:
            continue

        prefix, date_str = match.groups()
        try:
            col_date = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            continue

        age_days = (today - col_date).days
        if age_days > KEEP_DAYS:
            print(f"Archiving collection: {col.name} (age {age_days} days)")

            tmp_dir = tempfile.mkdtemp()
            parquet_path = os.path.join(tmp_dir, f"{col.name}.parquet")

            # Export data
            has_data = export_collection_to_parquet(client, col.name, parquet_path)

            if has_data and os.path.exists(parquet_path):
                # S3 key with prefix folder; upload raw Parquet
                s3_key = f"{prefix}/{os.path.basename(parquet_path)}"

                minio_client.upload_file(
                    parquet_path,
                    minio_bucket,
                    s3_key,
                    ExtraArgs={"ContentType": "application/x-parquet"}
                )
                print(f"Uploaded {parquet_path} to s3://{minio_bucket}/{s3_key}")

                # Delete from Qdrant
                client.delete_collection(collection_name=col.name, timeout=30)
                print(f"Deleted collection {col.name} from Qdrant")
            else:
                print(f"Skipped {col.name}, no data exported")

            shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            print(f"Keeping collection: {col.name} (age {age_days} days)")


if __name__ == "__main__":
    clean_and_archive_collections()
