from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import networkx as nx
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import squareform


SIMILARITY_FILE = Path("feed_similarity_matrix_last1000_per_feed.csv")
PCA_FILE = Path("feed_pca_coordinates_last1000_per_feed.csv")
DISPERSION_FILE = Path("feed_dispersion_last1000_per_feed.csv")
OUTPUT_DIR = Path("figures")

NETWORK_TOP_N = 20
HEATMAP_TOP_N = 15
SIMILARITY_THRESHOLD = 0.80
FIGURE_WIDTH = 1100
FIGURE_HEIGHT = 800


def load_data():
    """Load analysis tables and create one merged feed-level dataframe."""
    similarity_df = pd.read_csv(SIMILARITY_FILE, index_col=0)
    similarity_df = similarity_df.loc[~similarity_df.index.duplicated()]
    similarity_df = similarity_df.loc[:, ~similarity_df.columns.duplicated()]
    similarity_df = similarity_df.apply(pd.to_numeric, errors="coerce")

    common_labels = [label for label in similarity_df.index if label in similarity_df.columns]
    similarity_df = similarity_df.loc[common_labels, common_labels]

    pca_df = pd.read_csv(PCA_FILE)
    dispersion_df = pd.read_csv(DISPERSION_FILE)

    feed_df = pca_df.merge(
        dispersion_df[
            [
                "feed_display_name",
                "post_count",
                "mean_similarity_to_centroid",
                "similarity_std",
                "dispersion",
            ]
        ],
        on="feed_display_name",
        how="inner",
    )
    feed_df = feed_df.drop_duplicates(subset="feed_display_name").copy()

    return similarity_df, feed_df


def select_top_feeds(feed_df, similarity_df, top_n):
    """Select the highest-volume feeds that are present in all inputs."""
    available_feeds = set(similarity_df.index)
    filtered_df = feed_df[feed_df["feed_display_name"].isin(available_feeds)].copy()
    top_feeds = (
        filtered_df.sort_values("post_count", ascending=False)
        .head(top_n)["feed_display_name"]
        .tolist()
    )

    top_feed_df = filtered_df.set_index("feed_display_name").loc[top_feeds].reset_index()
    top_similarity_df = similarity_df.loc[top_feeds, top_feeds]
    return top_feed_df, top_similarity_df


def plot_pca_scatter(feed_df, output_dir):
    """Plot the semantic structure of feeds in the PCA space."""
    label_feeds = set(
        feed_df.sort_values("post_count", ascending=False)
        .head(5)["feed_display_name"]
        .tolist()
    )
    plot_df = feed_df.copy()
    plot_df["display_label"] = plot_df["feed_display_name"].where(
        plot_df["feed_display_name"].isin(label_feeds),
        "",
    )

    figure = px.scatter(
        plot_df,
        x="PC1",
        y="PC2",
        size="post_count",
        color="dispersion",
        text="display_label",
        hover_name="feed_display_name",
        hover_data={
            "feed_display_name": False,
            "post_count": True,
            "dispersion": ":.4f",
            "PC1": ":.3f",
            "PC2": ":.3f",
        },
        color_continuous_scale="Viridis",
        title="Semantic Structure of Left-Leaning Bluesky Feeds",
        template="plotly_white",
    )

    # Reference lines make the two semantic axes easier to interpret at a glance.
    figure.add_vline(x=0, line_width=1, line_dash="dash", line_color="rgba(90, 90, 90, 0.7)")
    figure.add_hline(y=0, line_width=1, line_dash="dash", line_color="rgba(90, 90, 90, 0.7)")
    figure.update_traces(
        marker=dict(line=dict(width=0.8, color="white"), opacity=0.9),
        textposition="top center",
    )
    figure.update_layout(
        xaxis_title="Primary Semantic Dimension (PC1)",
        yaxis_title="Secondary Semantic Dimension (PC2)",
        coloraxis_colorbar_title="Dispersion",
        width=FIGURE_WIDTH,
        height=FIGURE_HEIGHT,
    )

    figure.write_html(output_dir / "feed_pca_map.html")
    figure.write_image(output_dir / "feed_pca_map.png", scale=3)


def plot_dispersion_chart(feed_df, output_dir):
    """Plot feed-level semantic diversity sorted by dispersion."""
    sorted_df = feed_df.sort_values("dispersion", ascending=False).copy()

    # This chart emphasizes which feeds are internally broad versus semantically tight.
    figure = px.bar(
        sorted_df,
        x="feed_display_name",
        y="dispersion",
        color="dispersion",
        color_continuous_scale="Viridis",
        hover_data={
            "feed_display_name": True,
            "dispersion": ":.4f",
            "post_count": True,
        },
        title="Within-Feed Semantic Cohesion Across Political Feeds",
        template="plotly_white",
    )

    figure.update_traces(width=0.82)
    figure.update_layout(
        xaxis_title="Feed",
        yaxis_title="Within-Feed Semantic Diversity",
        coloraxis_showscale=False,
        width=FIGURE_WIDTH,
        height=FIGURE_HEIGHT,
    )
    figure.update_xaxes(tickangle=45)

    figure.write_html(output_dir / "feed_dispersion.html")
    figure.write_image(output_dir / "feed_dispersion.png", scale=3)


def build_similarity_network(similarity_df, threshold):
    """Create a weighted feed similarity graph from the cosine similarity matrix."""
    graph = nx.Graph()
    feed_names = list(similarity_df.index)

    for feed_name in feed_names:
        graph.add_node(feed_name)

    for i, feed_a in enumerate(feed_names):
        for j in range(i + 1, len(feed_names)):
            feed_b = feed_names[j]
            similarity_value = float(similarity_df.iloc[i, j])
            if similarity_value > threshold:
                graph.add_edge(feed_a, feed_b, weight=similarity_value)

    return graph


def detect_louvain_communities(graph):
    """Detect feed communities using the Louvain algorithm in networkx."""
    communities = nx.community.louvain_communities(graph, weight="weight", seed=42)

    community_map = {}
    for community_id, community_nodes in enumerate(communities):
        for node in community_nodes:
            community_map[node] = community_id

    return community_map


def plot_similarity_network(similarity_df, feed_df, output_dir, threshold=SIMILARITY_THRESHOLD):
    """Plot a force-directed network of semantic similarity between feeds."""
    graph = build_similarity_network(similarity_df, threshold=threshold)
    community_map = detect_louvain_communities(graph)
    positions = nx.spring_layout(graph, seed=42, weight="weight", k=0.8, iterations=200)
    label_nodes = set(
        pd.Series(nx.degree_centrality(graph))
        .sort_values(ascending=False)
        .head(5)
        .index
        .tolist()
    )

    feed_lookup = feed_df.set_index("feed_display_name")
    node_sizes = np.clip(feed_lookup.loc[list(graph.nodes()), "post_count"].to_numpy() / 10.0, 12, 55)
    node_colors = [community_map[node] for node in graph.nodes()]
    node_text = []

    for node in graph.nodes():
        node_text.append(
            "<br>".join(
                [
                    f"Feed: {node}",
                    f"Posts: {int(feed_lookup.loc[node, 'post_count'])}",
                    f"Dispersion: {feed_lookup.loc[node, 'dispersion']:.4f}",
                    f"Community: {community_map[node]}",
                ]
            )
        )

    edge_x = []
    edge_y = []
    for source, target in graph.edges():
        x0, y0 = positions[source]
        x1, y1 = positions[target]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode="lines",
        line=dict(width=1.2, color="rgba(120, 120, 120, 0.3)"),
        hoverinfo="none",
        showlegend=False,
    )

    node_labels = [node if node in label_nodes else "" for node in graph.nodes()]
    node_trace = go.Scatter(
        x=[positions[node][0] for node in graph.nodes()],
        y=[positions[node][1] for node in graph.nodes()],
        mode="markers+text",
        text=node_labels,
        textposition="top center",
        textfont=dict(size=8),
        hovertext=node_text,
        hoverinfo="text",
        marker=dict(
            size=node_sizes,
            color=node_colors,
            colorscale="Plasma",
            line=dict(width=1, color="white"),
            showscale=True,
            colorbar=dict(title="Community"),
        ),
        showlegend=False,
    )

    figure = go.Figure(data=[edge_trace, node_trace])
    figure.update_layout(
        title="Network of Semantic Similarity Between Political Feeds",
        template="plotly_white",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        margin=dict(l=20, r=20, t=60, b=20),
        width=FIGURE_WIDTH,
        height=FIGURE_HEIGHT,
    )

    figure.write_html(output_dir / "feed_similarity_network.html")
    figure.write_image(output_dir / "feed_similarity_network.png", scale=3)


def plot_similarity_heatmap(similarity_df, output_dir):
    """Plot a reduced heatmap ordered by similarity, without dendrogram branches."""
    distance_df = 1.0 - similarity_df
    condensed_distance = squareform(distance_df.to_numpy(), checks=False)
    linkage_matrix = linkage(condensed_distance, method="average")
    ordered_index = leaves_list(linkage_matrix)
    ordered_labels = similarity_df.index[ordered_index].tolist()
    ordered_df = similarity_df.loc[ordered_labels, ordered_labels]

    # The static heatmap is optimized for the paper-ready figure.
    plt.figure(figsize=(10, 8))
    ax = sns.heatmap(
        ordered_df,
        cmap="viridis",
        linewidths=0.2,
        cbar_kws={"label": "Cosine Similarity"},
    )
    ax.set_title("Semantic Similarity Between Political Feeds")
    ax.set_xlabel("Feed (columns)")
    ax.set_ylabel("Feed (rows)")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(output_dir / "feed_similarity_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close()

    heatmap_figure = px.imshow(
        ordered_df,
        color_continuous_scale="Viridis",
        aspect="auto",
        title="Semantic Similarity Between Political Feeds",
        labels={"x": "Feed (columns)", "y": "Feed (rows)", "color": "Cosine Similarity"},
    )
    heatmap_figure.update_layout(
        template="plotly_white",
        width=FIGURE_WIDTH,
        height=FIGURE_HEIGHT,
    )
    heatmap_figure.update_xaxes(tickangle=45)
    heatmap_figure.write_html(output_dir / "feed_similarity_heatmap.html")


def main():
    """Run the publication-quality feed clustering visualization pipeline."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pio.kaleido.scope.default_format = "png"

    similarity_df, feed_df = load_data()
    top_feed_df, top_similarity_df = select_top_feeds(feed_df, similarity_df, top_n=NETWORK_TOP_N)
    _, heatmap_similarity_df = select_top_feeds(feed_df, similarity_df, top_n=HEATMAP_TOP_N)

    plot_pca_scatter(top_feed_df, OUTPUT_DIR)
    plot_dispersion_chart(top_feed_df, OUTPUT_DIR)
    plot_similarity_network(top_similarity_df, top_feed_df, OUTPUT_DIR)
    plot_similarity_heatmap(heatmap_similarity_df, OUTPUT_DIR)


if __name__ == "__main__":
    main()
