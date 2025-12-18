from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from plotly.subplots import make_subplots

st.set_page_config(page_title="Voyla Executive Dashboard", layout="wide")

st.title("Voyla Executive Dashboard")
st.markdown("**Actionable Business Intelligence** - From Metrics to Decisions")


# Database connection
@st.cache_resource
def get_conn():
    try:
        conn = psycopg2.connect(
            "postgresql://postgres:We<3ProfKerger25@35.202.134.206:5432/postgres"
        )
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None


# Load main data with user signup dates
@st.cache_data(ttl=600)
def load_data():
    conn = psycopg2.connect(
        "postgresql://postgres:We<3ProfKerger25@35.202.134.206:5432/postgres"
    )
    query = """
    SELECT 
        i.id as interaction_id,
        i.user_id,
        u.display_name as username,
        u.email,
        u.created_at as user_signup_date,
        i.content_id,
        c.platform,
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
        i.created_at as interaction_date
    FROM interactions i
    LEFT JOIN users u ON i.user_id = u.id
    LEFT JOIN content c ON i.content_id = c.id
    LEFT JOIN places p ON i.place_id = p.id
    LEFT JOIN property pr ON i.property_id = pr.id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Connect and load
conn = get_conn()
if conn is None:
    st.stop()

with st.spinner("Loading data..."):
    df = load_data()

# Close the initial connection check
conn.close()

if df.empty:
    st.error("No data found in database")
    st.stop()

# Parse dates
df["interaction_date"] = pd.to_datetime(df["interaction_date"])
df["user_signup_date"] = pd.to_datetime(df["user_signup_date"])
df["date"] = df["interaction_date"].dt.date
df["interaction_month"] = df["interaction_date"].dt.to_period("M")
df["signup_month"] = df["user_signup_date"].dt.to_period("M")

# Create tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "Overview",
        "User Segmentation",
        "Geographic Expansion",
        "Monthly Growth",
        "Cohort Retention",
    ]
)

# ============================================================================
# TAB 1: OVERVIEW
# ============================================================================
with tab1:
    st.header("Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Users", f"{df['user_id'].nunique():,}")
    with col2:
        st.metric("Total Saves", f"{len(df):,}")
    with col3:
        st.metric("Unique Places", f"{df['place_id'].nunique():,}")
    with col4:
        avg_saves = len(df) / df["user_id"].nunique()
        st.metric("Avg Saves/User", f"{avg_saves:.1f}")

    st.markdown("---")

    # Daily and Monthly interaction timelines
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Daily Save Activity")
        daily_interactions = df.groupby("date").size().reset_index(name="count")
        fig = px.line(
            daily_interactions,
            x="date",
            y="count",
            title="Daily Saves Over Time",
            labels={"date": "Date", "count": "Number of Saves"},
        )
        fig.update_traces(line_color="#1f77b4")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Monthly Save Activity")
        monthly_interactions = (
            df.groupby("interaction_month").size().reset_index(name="count")
        )
        monthly_interactions["month"] = monthly_interactions[
            "interaction_month"
        ].astype(str)
        fig = px.bar(
            monthly_interactions,
            x="month",
            y="count",
            title="Monthly Saves",
            labels={"month": "Month", "count": "Saves"},
        )
        fig.update_traces(marker_color="#2ca02c")
        st.plotly_chart(fig, use_container_width=True)

    # Top active users and platform distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Most Active Users")
        user_activity = (
            df.groupby(["user_id", "username"]).size().reset_index(name="saves")
        )
        user_activity = user_activity.sort_values("saves", ascending=False).head(10)

        fig = px.bar(
            user_activity,
            x="saves",
            y="username",
            orientation="h",
            title="Top 10 Users by Saves",
            labels={"saves": "Number of Saves", "username": "User"},
        )
        fig.update_traces(marker_color="#ff7f0e")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Content Platform Distribution")
        if df["platform"].notna().sum() > 0:
            platform_counts = df["platform"].value_counts()
            fig = px.pie(
                values=platform_counts.values,
                names=platform_counts.index,
                title="Saves by Platform",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No platform data available")

# ============================================================================
# TAB 2: USER SEGMENTATION
# ============================================================================
with tab2:
    st.header("User Segmentation - Product Prioritization")
    st.markdown(
        "**Business Question:** Should we prioritize power user features or onboarding optimization?"
    )

    # Time period filter
    col1, col2 = st.columns([1, 3])
    with col1:
        time_filter = st.selectbox(
            "Time Period", ["All Time", "Last 90 Days", "Last 30 Days"], index=0
        )

    # Apply time filter
    df_filtered = df.copy()
    if time_filter == "Last 30 Days":
        cutoff_date = datetime.now() - timedelta(days=30)
        df_filtered = df_filtered[df_filtered["interaction_date"] >= cutoff_date]
    elif time_filter == "Last 90 Days":
        cutoff_date = datetime.now() - timedelta(days=90)
        df_filtered = df_filtered[df_filtered["interaction_date"] >= cutoff_date]

    # Calculate user saves and segments
    user_saves = df_filtered.groupby("user_id").size().reset_index(name="saves")

    def categorize_user(saves):
        if saves >= 100:
            return "Power User"
        elif saves >= 20:
            return "Casual User"
        else:
            return "New User"

    user_saves["segment"] = user_saves["saves"].apply(categorize_user)

    # Calculate segment statistics
    segment_counts = user_saves["segment"].value_counts()
    segment_stats = pd.DataFrame(
        {
            "Segment": segment_counts.index,
            "User Count": segment_counts.values,
            "% of Total": (segment_counts.values / len(user_saves) * 100).round(1),
        }
    )

    # Order segments properly
    segment_order = ["Power User", "Casual User", "New User"]
    segment_stats["Segment"] = pd.Categorical(
        segment_stats["Segment"], categories=segment_order, ordered=True
    )
    segment_stats = segment_stats.sort_values("Segment")

    # Define save ranges for display
    save_ranges = {
        "Power User": "100+ saves",
        "Casual User": "20-99 saves",
        "New User": "0-19 saves",
    }
    segment_stats["Saves Range"] = segment_stats["Segment"].map(save_ranges)

    # Display metrics
    col1, col2, col3 = st.columns(3)

    for idx, (col, segment) in enumerate(zip([col1, col2, col3], segment_order)):
        segment_data = segment_stats[segment_stats["Segment"] == segment]
        if not segment_data.empty:
            count = segment_data["User Count"].values[0]
            pct = segment_data["% of Total"].values[0]
            with col:
                st.metric(segment, f"{count:,} users", f"{pct}% of total")

    # Visualization
    col1, col2 = st.columns([2, 1])

    with col1:
        # Pie chart
        colors = {
            "Power User": "#d62728",
            "Casual User": "#ff7f0e",
            "New User": "#1f77b4",
        }
        fig = px.pie(
            segment_stats,
            values="User Count",
            names="Segment",
            title="User Distribution by Segment",
            color="Segment",
            color_discrete_map=colors,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Segment Details")
        st.dataframe(
            segment_stats[["Segment", "User Count", "% of Total", "Saves Range"]],
            hide_index=True,
            use_container_width=True,
        )

# ============================================================================
# TAB 3: GEOGRAPHIC EXPANSION
# ============================================================================
with tab3:
    st.header("Geographic Expansion - Market Strategy")
    st.markdown(
        "**Business Question:** Which market should we expand to next? When should we localize?"
    )

    # Calculate active users per country
    country_users = (
        df.groupby("country")
        .agg({"user_id": "nunique", "place_id": "nunique", "interaction_id": "count"})
        .reset_index()
    )
    country_users.columns = ["Country", "Active Users", "Unique Places", "Total Saves"]
    country_users = country_users.sort_values("Active Users", ascending=False).head(15)
    country_users["% of Total Users"] = (
        country_users["Active Users"] / df["user_id"].nunique() * 100
    ).round(1)

    # Display top 5 metrics
    st.subheader("Top 5 Markets")
    cols = st.columns(5)
    for i, (idx, row) in enumerate(country_users.head(5).iterrows()):
        with cols[i]:
            st.metric(
                row["Country"],
                f"{row['Active Users']:,} users",
                f"{row['% of Total Users']}%",
            )

    st.markdown("---")

    # Bar chart
    fig = px.bar(
        country_users,
        x="Active Users",
        y="Country",
        orientation="h",
        title="Top 15 Countries by Active Users",
        labels={"Active Users": "Number of Active Users", "Country": "Country"},
        text="Active Users",
    )
    fig.update_traces(
        marker_color="#2ca02c", texttemplate="%{text:,}", textposition="outside"
    )
    fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.subheader("Market Details")
    st.dataframe(
        country_users[
            [
                "Country",
                "Active Users",
                "% of Total Users",
                "Unique Places",
                "Total Saves",
            ]
        ],
        hide_index=True,
        use_container_width=True,
    )

# ============================================================================
# TAB 4: MONTHLY GROWTH
# ============================================================================
with tab4:
    st.header("Monthly Growth Trajectory - Viral Mechanism Analysis")
    st.markdown(
        "**Business Question:** What drove growth spikes? Can we replicate them?"
    )

    # Calculate new users per month (by signup date)
    new_users_monthly = df.groupby("signup_month")["user_id"].nunique().reset_index()
    new_users_monthly.columns = ["Month", "New Users"]
    new_users_monthly["Month"] = new_users_monthly["Month"].astype(str)
    new_users_monthly["Cumulative Users"] = new_users_monthly["New Users"].cumsum()

    # Calculate total interactions per month
    interactions_monthly = (
        df.groupby("interaction_month").size().reset_index(name="Total Interactions")
    )
    interactions_monthly["Month"] = interactions_monthly["interaction_month"].astype(
        str
    )

    # Merge datasets
    growth_data = new_users_monthly.merge(
        interactions_monthly[["Month", "Total Interactions"]], on="Month", how="left"
    )
    growth_data["Total Interactions"] = (
        growth_data["Total Interactions"].fillna(0).astype(int)
    )

    # Calculate growth rates
    growth_data["Growth Rate"] = growth_data["New Users"].pct_change() * 100
    growth_data["Growth Rate"] = growth_data["Growth Rate"].fillna(0).round(1)

    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)

    total_users = growth_data["New Users"].sum()
    total_interactions = growth_data["Total Interactions"].sum()
    peak_month = growth_data.loc[growth_data["New Users"].idxmax()]
    latest_month = growth_data.iloc[-1]

    with col1:
        st.metric("Total Users", f"{total_users:,}")
    with col2:
        st.metric("Total Interactions", f"{total_interactions:,}")
    with col3:
        st.metric(
            "Peak Growth Month",
            peak_month["Month"],
            f"{peak_month['New Users']:,} users",
        )
    with col4:
        st.metric("Latest Month Growth", f"{latest_month['Growth Rate']:.1f}%")

    st.markdown("---")

    # Dual-axis chart
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add new users bars
    fig.add_trace(
        go.Bar(
            x=growth_data["Month"],
            y=growth_data["New Users"],
            name="New Users",
            marker_color="#1f77b4",
            text=growth_data["New Users"],
            texttemplate="%{text:,}",
            textposition="outside",
        ),
        secondary_y=False,
    )

    # Add cumulative users line
    fig.add_trace(
        go.Scatter(
            x=growth_data["Month"],
            y=growth_data["Cumulative Users"],
            name="Cumulative Users",
            mode="lines+markers",
            line=dict(color="#ff7f0e", width=3),
            marker=dict(size=8),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title="Monthly Growth: New Users & Cumulative Total",
        xaxis_title="Month",
        hovermode="x unified",
        height=500,
    )

    fig.update_yaxes(title_text="New Users (Bars)", secondary_y=False)
    fig.update_yaxes(title_text="Cumulative Users (Line)", secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)

    # Growth data table
    st.subheader("Monthly Growth Details")
    growth_display = growth_data[
        ["Month", "New Users", "Cumulative Users", "Growth Rate", "Total Interactions"]
    ].copy()
    growth_display["Growth Rate"] = growth_display["Growth Rate"].apply(
        lambda x: f"{x:+.1f}%"
    )
    st.dataframe(growth_display, hide_index=True, use_container_width=True)

# ============================================================================
# TAB 5: COHORT RETENTION
# ============================================================================
with tab5:
    st.header("Cohort Retention Analysis - Re-engagement Timing")
    st.markdown(
        "**Business Question:** When should we trigger re-engagement notifications to prevent churn?"
    )

    # Get unique users with their signup month
    user_cohorts = df[["user_id", "signup_month"]].drop_duplicates()

    # For each user, find which months they were active (had interactions)
    user_activity = (
        df.groupby(["user_id", "interaction_month"])
        .size()
        .reset_index(name="interactions")
    )

    # Merge to get signup month for each activity
    user_activity = user_activity.merge(user_cohorts, on="user_id")

    # Calculate months since signup
    user_activity["months_since_signup"] = (
        user_activity["interaction_month"].dt.to_timestamp().dt.to_period("M")
        - user_activity["signup_month"]
    ).apply(lambda x: x.n)

    # Create cohort retention matrix
    cohort_data = []
    cohort_months = sorted(df["signup_month"].unique())

    for cohort in cohort_months:
        cohort_users = user_cohorts[user_cohorts["signup_month"] == cohort]
        cohort_size = len(cohort_users)

        row = {"Cohort": str(cohort), "Cohort Size": cohort_size}

        # Calculate retention for M0, M1, M3, M6
        for month_offset in [0, 1, 3, 6]:
            active_users = user_activity[
                (user_activity["signup_month"] == cohort)
                & (user_activity["months_since_signup"] == month_offset)
            ]["user_id"].nunique()

            retention_pct = (active_users / cohort_size * 100) if cohort_size > 0 else 0
            row[f"M{month_offset}"] = retention_pct

        cohort_data.append(row)

    cohort_df = pd.DataFrame(cohort_data)

    # Display summary metrics
    if not cohort_df.empty:
        col1, col2, col3, col4 = st.columns(4)

        avg_m0 = cohort_df["M0"].mean()
        avg_m1 = cohort_df["M1"].mean()
        avg_m3 = cohort_df["M3"].mean() if "M3" in cohort_df.columns else 0
        avg_m6 = cohort_df["M6"].mean() if "M6" in cohort_df.columns else 0

        with col1:
            st.metric("Avg M0 Retention", f"{avg_m0:.1f}%")
        with col2:
            st.metric("Avg M1 Retention", f"{avg_m1:.1f}%", f"{avg_m1 - avg_m0:.1f}%")
        with col3:
            if avg_m3 > 0:
                st.metric(
                    "Avg M3 Retention", f"{avg_m3:.1f}%", f"{avg_m3 - avg_m1:.1f}%"
                )
            else:
                st.metric("Avg M3 Retention", "N/A")
        with col4:
            if avg_m6 > 0:
                st.metric(
                    "Avg M6 Retention", f"{avg_m6:.1f}%", f"{avg_m6 - avg_m3:.1f}%"
                )
            else:
                st.metric("Avg M6 Retention", "N/A")

    st.markdown("---")

    # Create heatmap
    if not cohort_df.empty:
        # Prepare data for heatmap
        retention_cols = ["M0", "M1", "M3", "M6"]
        available_cols = [col for col in retention_cols if col in cohort_df.columns]

        heatmap_data = cohort_df[["Cohort"] + available_cols].set_index("Cohort")

        fig = go.Figure(
            data=go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                colorscale="RdYlGn",
                text=heatmap_data.values.round(1),
                texttemplate="%{text}%",
                textfont={"size": 12},
                colorbar=dict(title="Retention %"),
            )
        )

        fig.update_layout(
            title="Cohort Retention Heatmap",
            xaxis_title="Months Since Signup",
            yaxis_title="Signup Cohort",
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)

    # Retention table
    st.subheader("Cohort Retention Details")
    display_df = cohort_df.copy()
    for col in ["M0", "M1", "M3", "M6"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%")
    st.dataframe(display_df, hide_index=True, use_container_width=True)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**Voyla Executive Dashboard** v3.0")
st.sidebar.info(f"""
**Data Summary**
- {len(df):,} total saves
- {df["user_id"].nunique():,} users
- {df["place_id"].nunique():,} places
- Date Range: {df["date"].min()} to {df["date"].max()}
""")
