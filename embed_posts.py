import os
import time

import numpy as np
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer


INPUT_CSV = "raw_posts_2weeks.csv"
OUTPUT_NPY = "embeddings_2weeks.npy"


def load_data():
    """Load text data from CSV and return list preserving row order."""
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    if "text" not in df.columns:
        raise ValueError("Required column 'text' not found in raw_posts_2weeks.csv")

    df["text"] = df["text"].fillna("")
    texts = df["text"].astype(str).tolist()

    return texts


def generate_embeddings(texts):
    """Generate normalized sentence embeddings using MPS when available."""
    device = "mps" if torch.backends.mps.is_available() else "cpu"
    print(f"Using device: {device}")

    if len(texts) > 200000:
        print("Warning: Dataset has more than 200,000 rows. Consider reducing batch_size to 64.")

    torch.set_float32_matmul_precision("high")
    model = SentenceTransformer("all-MiniLM-L6-v2", device=device)

    start_time = time.time()

    with torch.no_grad():
        embeddings = model.encode(
            texts,
            batch_size=128,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

    runtime_seconds = time.time() - start_time
    return embeddings, runtime_seconds


def main():
    texts = load_data()
    embeddings, runtime_seconds = generate_embeddings(texts)

    np.save(OUTPUT_NPY, embeddings)

    total_rows = embeddings.shape[0]
    embedding_dim = embeddings.shape[1] if embeddings.ndim == 2 else 0
    memory_mb = embeddings.nbytes / (1024 * 1024)

    print(f"Total rows embedded: {total_rows}")
    print(f"Embedding vector dimension: {embedding_dim}")
    print(f"Total runtime in seconds: {runtime_seconds:.2f}")
    print(f"Approximate memory usage of embedding array (in MB): {memory_mb:.2f}")


if __name__ == "__main__":
    main()
