import os
import re

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

load_dotenv()


# Function to remove emojis from text
def remove_emojis(text):
    if pd.isna(text):
        return text
    emoji_pattern = re.compile(
        "["
        "\U0001f600-\U0001f64f"  # emoticons
        "\U0001f300-\U0001f5ff"  # symbols & pictographs
        "\U0001f680-\U0001f6ff"  # transport & map symbols
        "\U0001f1e0-\U0001f1ff"  # flags (iOS)
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "\U0001f900-\U0001f9ff"  # supplemental symbols
        "\U00002600-\U000026ff"  # misc symbols
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", text).strip()


st.set_page_config(page_title="Voyla Dashboard", layout="wide")

# Title
st.title("Voyla Analytics Dashboard")
st.markdown("User behavior and interaction analysis")


# Database connection
@st.cache_resource
def get_conn():
    try:
        conn = psycopg2.connect(os.getenv("OLAP_DATABASE_URL"))
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None


# Load data
@st.cache_data(ttl=600)
def load_data(_conn):
    query = """
    SELECT 
        i.id as interaction_id,
        i.user_id,
        u.display_name as username,
        u.email,
        i.content_id,
        c.platform,
        c.url as content_url,
        i.place_id,
        p.english_display_name as place_name,
        p.english_locality as city,
        p.english_administrative_area as state,
        p.country_code as country,
        p.latitude,
        p.longitude,
        p.rating as place_rating,
        p.primary_type as place_type,
        i.property_id,
        pr.english_name as property_name,
        pr.category_type as property_category,
        pr.emoji as property_emoji,
        i.created_at as interaction_date
    FROM interactions i
    LEFT JOIN users u ON i.user_id = u.id
    LEFT JOIN content c ON i.content_id = c.id
    LEFT JOIN places p ON i.place_id = p.id
    LEFT JOIN property pr ON i.property_id = pr.id
    """
    return pd.read_sql(query, _conn)


# Connect and load
conn = get_conn()
if conn is None:
    st.stop()

with st.spinner("Loading data..."):
    df = load_data(conn)

if df.empty:
    st.error("No data found in database")
    st.stop()

# Remove emojis from text columns
text_columns = [
    "username",
    "place_name",
    "city",
    "state",
    "property_name",
    "property_category",
]
for col in text_columns:
    if col in df.columns:
        df[col] = df[col].apply(remove_emojis)

# Parse dates
df["interaction_date"] = pd.to_datetime(df["interaction_date"])
df["date"] = df["interaction_date"].dt.date
df["month"] = df["interaction_date"].dt.to_period("M").astype(str)

# Sidebar controls
st.sidebar.header("Controls")
n_clusters = st.sidebar.slider("Number of User Clusters", 2, 10, 4)
show_data = st.sidebar.checkbox("Show Raw Data")

# Key metrics
st.header("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Users", f"{df['user_id'].nunique():,}")
with col2:
    st.metric("Total Interactions", f"{len(df):,}")
with col3:
    st.metric("Unique Places", f"{df['place_id'].nunique():,}")
with col4:
    st.metric("Unique Properties", f"{df['property_id'].nunique():,}")

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(
    ["Activity Overview", "User Clusters", "Geography", "Properties"]
)

# Tab 1: Activity Overview
with tab1:
    st.subheader("Interaction Timeline")

    # Daily interactions
    daily_interactions = df.groupby("date").size().reset_index(name="count")
    fig = px.line(
        daily_interactions,
        x="date",
        y="count",
        title="Daily Interactions",
        labels={"date": "Date", "count": "Number of Interactions"},
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        # Monthly interactions
        monthly_interactions = df.groupby("month").size().reset_index(name="count")
        fig = px.bar(
            monthly_interactions,
            x="month",
            y="count",
            title="Monthly Interactions",
            labels={"month": "Month", "count": "Interactions"},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Platform distribution
        if df["platform"].notna().sum() > 0:
            platform_counts = df["platform"].value_counts()
            fig = px.pie(
                values=platform_counts.values,
                names=platform_counts.index,
                title="Content by Platform",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No platform data available")

    # Top active users
    st.subheader("Most Active Users")
    user_activity = (
        df.groupby(["user_id", "username"]).size().reset_index(name="interactions")
    )
    user_activity = user_activity.sort_values("interactions", ascending=False).head(10)

    fig = px.bar(
        user_activity,
        x="interactions",
        y="username",
        orientation="h",
        title="Top 10 Users by Interactions",
        labels={"interactions": "Number of Interactions", "username": "User"},
    )
    st.plotly_chart(fig, use_container_width=True)

# Tab 2: User Clustering
with tab2:
    st.subheader("User Behavior Clustering")

    # Aggregate by user
    user_stats = (
        df.groupby("user_id")
        .agg(
            {
                "interaction_id": "count",  # Total interactions
                "place_id": "nunique",  # Unique places visited
                "property_id": "nunique",  # Unique properties interacted with
                "content_id": "nunique",  # Unique content
            }
        )
        .reset_index()
    )

    user_stats.columns = [
        "user_id",
        "total_interactions",
        "unique_places",
        "unique_properties",
        "unique_content",
    ]

    # Only cluster if we have enough users
    if len(user_stats) < n_clusters:
        st.warning(
            f"Not enough users ({len(user_stats)}) for {n_clusters} clusters. Showing data without clustering."
        )
        st.dataframe(user_stats, use_container_width=True)
    else:
        # Perform clustering
        features = [
            "total_interactions",
            "unique_places",
            "unique_properties",
            "unique_content",
        ]
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(user_stats[features])

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        user_stats["cluster"] = kmeans.fit_predict(scaled_features)

        # Show cluster sizes
        cluster_counts = user_stats["cluster"].value_counts().sort_index()
        st.write(f"**Cluster Distribution:** {dict(cluster_counts)}")

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            fig = px.scatter(
                user_stats,
                x="total_interactions",
                y="unique_places",
                color="cluster",
                size="unique_properties",
                title="User Clusters: Activity vs Exploration",
                labels={
                    "total_interactions": "Total Interactions",
                    "unique_places": "Unique Places Visited",
                    "cluster": "Cluster",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.pie(
                values=cluster_counts.values,
                names=cluster_counts.index,
                title="Users per Cluster",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Cluster statistics
        st.subheader("Cluster Characteristics")
        cluster_summary = user_stats.groupby("cluster")[features].mean().round(2)
        cluster_summary["user_count"] = cluster_counts
        st.dataframe(cluster_summary, use_container_width=True)

        # Cluster profiles
        st.subheader("Cluster Profiles")
        for cluster in sorted(user_stats["cluster"].unique()):
            stats = cluster_summary.loc[cluster]
            if stats["total_interactions"] > user_stats["total_interactions"].median():
                profile = "High Activity"
            else:
                profile = "Casual User"

            if stats["unique_places"] > user_stats["unique_places"].median():
                profile += ", Explorer"
            else:
                profile += ", Focused"

            st.write(
                f"**Cluster {cluster}**: {profile} ({int(stats['user_count'])} users)"
            )

# Tab 3: Geography
with tab3:
    st.subheader("Geographic Distribution")

    col1, col2 = st.columns(2)

    with col1:
        # Top cities
        if df["city"].notna().sum() > 0:
            city_counts = df["city"].value_counts().head(10)
            fig = px.bar(
                x=city_counts.values,
                y=city_counts.index,
                orientation="h",
                title="Top 10 Cities by Activity",
                labels={"x": "Number of Interactions", "y": "City"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No city data available")

    with col2:
        # Top states
        if df["state"].notna().sum() > 0:
            state_counts = df["state"].value_counts().head(10)
            fig = px.bar(
                x=state_counts.values,
                y=state_counts.index,
                orientation="h",
                title="Top 10 States by Activity",
                labels={"x": "Number of Interactions", "y": "State"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No state data available")

    # Place types
    st.subheader("Popular Place Types")
    if df["place_type"].notna().sum() > 0:
        place_type_counts = df["place_type"].value_counts().head(15)
        fig = px.bar(
            x=place_type_counts.values,
            y=place_type_counts.index,
            orientation="h",
            title="Top 15 Place Types",
            labels={"x": "Number of Interactions", "y": "Place Type"},
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No place type data available")

    # Top rated places
    st.subheader("Highly Rated Places")
    if df["place_rating"].notna().sum() > 0:
        top_places = (
            df[df["place_rating"].notna()]
            .groupby("place_name")
            .agg({"place_rating": "first", "interaction_id": "count"})
            .rename(columns={"interaction_id": "interactions"})
        )
        top_places = (
            top_places[top_places["interactions"] >= 2]
            .sort_values("place_rating", ascending=False)
            .head(10)
        )

        fig = px.bar(
            top_places.reset_index(),
            x="place_rating",
            y="place_name",
            orientation="h",
            title="Top Rated Places (with 2+ interactions)",
            labels={"place_rating": "Rating", "place_name": "Place"},
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No rating data available")

# Tab 4: Properties
with tab4:
    st.subheader("Property Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Property categories
        if df["property_category"].notna().sum() > 0:
            category_counts = df["property_category"].value_counts()
            fig = px.pie(
                values=category_counts.values,
                names=category_counts.index,
                title="Distribution by Property Category",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No property category data available")

    with col2:
        # Top properties
        property_counts = df["property_name"].value_counts().head(10)
        fig = px.bar(
            x=property_counts.values,
            y=property_counts.index,
            orientation="h",
            title="Top 10 Properties by Interactions",
            labels={"x": "Interactions", "y": "Property"},
        )
        st.plotly_chart(fig, use_container_width=True)

    # Property interaction summary
    st.subheader("Popular Property Types")
    if df["property_name"].notna().sum() > 0:
        property_summary = df.groupby("property_name").size().reset_index(name="count")
        property_summary = property_summary.sort_values("count", ascending=False).head(
            20
        )

        st.write("Most popular properties:")
        for _, row in property_summary.iterrows():
            st.write(f"**{row['property_name']}**: {row['count']} interactions")

# Show raw data if requested
if show_data:
    st.header("Raw Data")
    st.dataframe(df, use_container_width=True)

    # Download button
    csv = df.to_csv(index=False)
    st.download_button(
        "Download Data as CSV", csv, "voyla_interactions.csv", "text/csv"
    )

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**Voyla Analytics** v2.0")
st.sidebar.info(
    f"{len(df):,} interactions | {df['user_id'].nunique()} users | {df['place_id'].nunique()} places"
)
