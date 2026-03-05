import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.metrics.pairwise import cosine_similarity


INPUT_CSV = "raw_posts_last1000_per_feed.csv"
INPUT_EMBEDDINGS = "embeddings_last1000_per_feed.npy"
OUTPUT_DISPERSION = "feed_dispersion_last1000_per_feed.csv"
OUTPUT_SIMILARITY = "feed_similarity_matrix_last1000_per_feed.csv"
OUTPUT_PCA = "feed_pca_coordinates_last1000_per_feed.csv"


def load_data():
    """Load posts and embeddings, validate row alignment, and add embedding index."""
    posts_df = pd.read_csv(INPUT_CSV)
    embeddings = np.load(INPUT_EMBEDDINGS)

    if len(posts_df) != len(embeddings):
        raise ValueError(
            f"Row count mismatch: CSV has {len(posts_df)} rows, embeddings has {len(embeddings)} rows"
        )

    required_cols = {"feed_at_uri", "feed_display_name"}
    missing_cols = required_cols - set(posts_df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in CSV: {', '.join(sorted(missing_cols))}")

    posts_df = posts_df.copy()
    posts_df["embedding_index"] = np.arange(len(posts_df))

    return posts_df, embeddings


def compute_feed_centroids(posts_df, embeddings):
    """Compute mean embedding vector and post count for each feed."""
    centroids = {}
    feed_sizes = {}

    for feed_at_uri in posts_df["feed_at_uri"].dropna().unique():
        feed_rows = posts_df[posts_df["feed_at_uri"] == feed_at_uri]
        idx = feed_rows["embedding_index"].to_numpy()
        feed_emb = embeddings[idx]

        if len(feed_emb) == 0:
            continue

        centroids[feed_at_uri] = feed_emb.mean(axis=0)
        feed_sizes[feed_at_uri] = len(feed_emb)

    return centroids, feed_sizes


def compute_dispersion(posts_df, embeddings, centroids, feed_sizes):
    """Compute per-feed cohesion statistics.

    Dispersion here is defined as 1 - mean cosine similarity between posts and
    their feed centroid. Lower dispersion means tighter semantic cohesion.
    """
    rows = []

    for feed_at_uri, centroid in centroids.items():
        feed_rows = posts_df[posts_df["feed_at_uri"] == feed_at_uri]
        idx = feed_rows["embedding_index"].to_numpy()
        feed_emb = embeddings[idx]

        sims = cosine_similarity(feed_emb, centroid.reshape(1, -1)).ravel()
        mean_sim = float(np.mean(sims))
        std_sim = float(np.std(sims))
        dispersion = 1.0 - mean_sim

        display_name = feed_rows["feed_display_name"].dropna()
        feed_display_name = display_name.iloc[0] if not display_name.empty else ""

        rows.append(
            {
                "feed_at_uri": feed_at_uri,
                "feed_display_name": feed_display_name,
                "post_count": feed_sizes[feed_at_uri],
                "mean_similarity_to_centroid": mean_sim,
                "similarity_std": std_sim,
                "dispersion": dispersion,
            }
        )

    dispersion_df = pd.DataFrame(rows).sort_values(
        by="dispersion", ascending=True
    )
    dispersion_df.to_csv(OUTPUT_DISPERSION, index=False)

    return dispersion_df


def compute_similarity_matrix(posts_df, centroids):
    """Compute centroid-to-centroid cosine similarity matrix and summary stats."""
    feed_order = list(centroids.keys())
    centroid_matrix = np.vstack([centroids[feed] for feed in feed_order])

    sim_matrix = cosine_similarity(centroid_matrix)

    display_map = (
        posts_df[["feed_at_uri", "feed_display_name"]]
        .dropna(subset=["feed_at_uri"])
        .drop_duplicates(subset=["feed_at_uri"])
        .set_index("feed_at_uri")["feed_display_name"]
        .to_dict()
    )

    labels = [display_map.get(feed, feed) for feed in feed_order]
    sim_df = pd.DataFrame(sim_matrix, index=labels, columns=labels)
    sim_df.to_csv(OUTPUT_SIMILARITY)

    n = sim_matrix.shape[0]
    if n < 2:
        print("Not enough feeds to compute inter-feed similarity stats.")
        return sim_df, centroid_matrix, feed_order

    mask = ~np.eye(n, dtype=bool)
    off_diag_vals = sim_matrix[mask]
    avg_inter_feed = float(np.mean(off_diag_vals))

    upper = np.triu_indices(n, k=1)
    upper_vals = sim_matrix[upper]

    max_idx = int(np.argmax(upper_vals))
    min_idx = int(np.argmin(upper_vals))

    i_max, j_max = upper[0][max_idx], upper[1][max_idx]
    i_min, j_min = upper[0][min_idx], upper[1][min_idx]

    print(f"Average inter-feed similarity (excluding diagonal): {avg_inter_feed:.4f}")
    print(
        "Most similar feed pair: "
        f"{labels[i_max]} <-> {labels[j_max]} "
        f"(similarity={sim_matrix[i_max, j_max]:.4f})"
    )
    print(
        "Least similar feed pair: "
        f"{labels[i_min]} <-> {labels[j_min]} "
        f"(similarity={sim_matrix[i_min, j_min]:.4f})"
    )

    return sim_df, centroid_matrix, feed_order


def compute_pca(posts_df, centroid_matrix, feed_order):
    """Project feed centroids to 2D.

    PCA summarizes dominant axes of semantic variation across feed centroids,
    so PC1/PC2 give a compact map of how feeds differ in embedding space.
    """
    pca = PCA(n_components=2)
    coords = pca.fit_transform(centroid_matrix)

    display_map = (
        posts_df[["feed_at_uri", "feed_display_name"]]
        .dropna(subset=["feed_at_uri"])
        .drop_duplicates(subset=["feed_at_uri"])
        .set_index("feed_at_uri")["feed_display_name"]
        .to_dict()
    )

    pca_df = pd.DataFrame(
        {
            "feed_at_uri": feed_order,
            "feed_display_name": [display_map.get(feed, "") for feed in feed_order],
            "PC1": coords[:, 0],
            "PC2": coords[:, 1],
        }
    )
    pca_df.to_csv(OUTPUT_PCA, index=False)

    evr = pca.explained_variance_ratio_
    total_2d = float(np.sum(evr))
    print(f"Explained variance ratio PC1: {evr[0]:.4f}")
    print(f"Explained variance ratio PC2: {evr[1]:.4f}")
    print(f"Total variance explained by first two components: {total_2d:.4f}")

    return pca_df


def main():
    posts_df, embeddings = load_data()

    centroids, feed_sizes = compute_feed_centroids(posts_df, embeddings)
    if not centroids:
        raise ValueError("No valid feeds found to analyze.")

    compute_dispersion(posts_df, embeddings, centroids, feed_sizes)
    _, centroid_matrix, feed_order = compute_similarity_matrix(posts_df, centroids)
    compute_pca(posts_df, centroid_matrix, feed_order)

    print(f"Saved: {OUTPUT_DISPERSION}")
    print(f"Saved: {OUTPUT_SIMILARITY}")
    print(f"Saved: {OUTPUT_PCA}")


if __name__ == "__main__":
    main()
