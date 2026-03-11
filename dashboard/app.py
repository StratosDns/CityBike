from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PUBLISHED_DIR = PROJECT_ROOT / "data" / "published"


st.set_page_config(
    page_title="Citi Bike Demand Monitor",
    page_icon="🚲",
    layout="wide",
)


def read_csv(filename: str) -> pd.DataFrame:
    """
    Read a published CSV file from the data/published directory.
    """
    path = PUBLISHED_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Published file not found: {path}")
    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_data() -> dict[str, pd.DataFrame]:
    """
    Load all published dashboard datasets.
    """
    datasets = {
        "daily_kpis": read_csv("daily_kpis.csv"),
        "hourly_demand": read_csv("hourly_demand.csv"),
        "member_vs_casual": read_csv("member_vs_casual.csv"),
        "top_start_stations": read_csv("top_start_stations.csv"),
        "top_end_stations": read_csv("top_end_stations.csv"),
        "station_imbalance": read_csv("station_imbalance.csv"),
        "duration_summary": read_csv("duration_summary.csv"),
    }

    datasets["daily_kpis"]["ride_date"] = pd.to_datetime(datasets["daily_kpis"]["ride_date"])
    return datasets


def format_int(value: int | float) -> str:
    """
    Format a numeric value as an integer with thousands separators.
    """
    return f"{int(round(value)):,}"


def format_float(value: int | float, decimals: int = 2) -> str:
    """
    Format a numeric value with a fixed number of decimals.
    """
    return f"{float(value):,.{decimals}f}"


def build_kpi_row(daily_kpis: pd.DataFrame) -> None:
    """
    Render the top-level KPI cards.
    """
    total_rides = int(daily_kpis["total_rides"].sum())
    avg_daily_rides = float(daily_kpis["total_rides"].mean())
    avg_duration = float(daily_kpis["avg_ride_duration_minutes"].mean())
    avg_member_share = float(daily_kpis["member_share"].mean())

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total rides", format_int(total_rides))
    col2.metric("Average daily rides", format_int(avg_daily_rides))
    col3.metric("Average ride duration (min)", format_float(avg_duration))
    col4.metric("Average member share", f"{avg_member_share:.1%}")


def plot_daily_rides(daily_kpis: pd.DataFrame) -> None:
    """
    Plot daily ridership volume.
    """
    fig = px.line(
        daily_kpis,
        x="ride_date",
        y="total_rides",
        title="Daily ridership trend",
        labels={"ride_date": "Date", "total_rides": "Total rides"},
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_member_mix(member_vs_casual: pd.DataFrame) -> None:
    """
    Plot monthly rider mix by member type.
    """
    fig = px.bar(
        member_vs_casual,
        x="year_month",
        y="total_rides",
        color="member_type",
        barmode="stack",
        title="Monthly rider mix",
        labels={
            "year_month": "Month",
            "total_rides": "Total rides",
            "member_type": "Rider type",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_hourly_pattern(hourly_demand: pd.DataFrame, selected_day: str) -> None:
    """
    Plot hourly demand for a selected day.
    """
    filtered = hourly_demand[hourly_demand["day_name"] == selected_day].copy()
    filtered = filtered.sort_values(["start_hour", "member_type"])

    fig = px.line(
        filtered,
        x="start_hour",
        y="total_rides",
        color="member_type",
        markers=True,
        title=f"Hourly demand pattern — {selected_day}",
        labels={
            "start_hour": "Hour of day",
            "total_rides": "Total rides",
            "member_type": "Rider type",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_top_stations(top_start_stations: pd.DataFrame, top_n: int) -> None:
    """
    Plot the top start stations by ride volume.
    """
    filtered = top_start_stations.head(top_n).copy()
    filtered = filtered.sort_values("total_starts", ascending=True)

    fig = px.bar(
        filtered,
        x="total_starts",
        y="start_station_name",
        orientation="h",
        title=f"Top {top_n} start stations",
        labels={
            "total_starts": "Trip starts",
            "start_station_name": "Station",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_station_imbalance(station_imbalance: pd.DataFrame, top_n: int) -> None:
    """
    Plot the most imbalanced stations by net imbalance.
    """
    filtered = station_imbalance.copy()
    filtered["abs_net_imbalance"] = filtered["net_imbalance"].abs()
    filtered = filtered.sort_values("abs_net_imbalance", ascending=False).head(top_n)
    filtered = filtered.sort_values("net_imbalance", ascending=True)

    fig = px.bar(
        filtered,
        x="net_imbalance",
        y="station_name",
        orientation="h",
        title=f"Top {top_n} station imbalances",
        labels={
            "net_imbalance": "Net imbalance (starts - ends)",
            "station_name": "Station",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def show_duration_table(duration_summary: pd.DataFrame) -> None:
    """
    Display ride duration summary by rider segment.
    """
    display_df = duration_summary.copy()
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def show_business_insights(
    daily_kpis: pd.DataFrame,
    member_vs_casual: pd.DataFrame,
    station_imbalance: pd.DataFrame,
) -> None:
    """
    Render a compact business insights section based on published datasets.
    """
    busiest_day = daily_kpis.loc[daily_kpis["total_rides"].idxmax()]
    strongest_member_month = member_vs_casual.loc[member_vs_casual["ride_share"].idxmax()]
    most_positive_imbalance = station_imbalance.loc[station_imbalance["net_imbalance"].idxmax()]
    most_negative_imbalance = station_imbalance.loc[station_imbalance["net_imbalance"].idxmin()]

    st.subheader("Business insights")
    st.markdown(
        f"""
**Peak demand day:** {busiest_day["ride_date"].date()} had {format_int(busiest_day["total_rides"])} rides.

**Highest rider-share segment:** {strongest_member_month["member_type"]} reached {strongest_member_month["ride_share"]:.1%} share in {strongest_member_month["year_month"]}.

**Most export-heavy station:** {most_positive_imbalance["station_name"]} posted a net imbalance of {format_int(most_positive_imbalance["net_imbalance"])}.

**Most import-heavy station:** {most_negative_imbalance["station_name"]} posted a net imbalance of {format_int(most_negative_imbalance["net_imbalance"])}.
"""
    )


def main() -> None:
    """
    Render the full dashboard.
    """
    st.title("Citi Bike Demand Monitor")
    st.caption("Public-data analytics dashboard powered by published gold-layer outputs.")

    try:
        data = load_data()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    daily_kpis = data["daily_kpis"]
    hourly_demand = data["hourly_demand"]
    member_vs_casual = data["member_vs_casual"]
    top_start_stations = data["top_start_stations"]
    station_imbalance = data["station_imbalance"]
    duration_summary = data["duration_summary"]

    with st.sidebar:
        st.header("Controls")

        available_days = sorted(hourly_demand["day_name"].dropna().unique().tolist())
        selected_day = st.selectbox("Day of week", available_days, index=0)

        top_n_stations = st.slider("Top stations", min_value=5, max_value=30, value=15, step=5)
        top_n_imbalance = st.slider("Top imbalances", min_value=5, max_value=30, value=15, step=5)

    build_kpi_row(daily_kpis)

    col1, col2 = st.columns(2)
    with col1:
        plot_daily_rides(daily_kpis)
    with col2:
        plot_member_mix(member_vs_casual)

    col3, col4 = st.columns(2)
    with col3:
        plot_hourly_pattern(hourly_demand, selected_day)
    with col4:
        plot_top_stations(top_start_stations, top_n_stations)

    plot_station_imbalance(station_imbalance, top_n_imbalance)
    show_duration_table(duration_summary)
    show_business_insights(daily_kpis, member_vs_casual, station_imbalance)


if __name__ == "__main__":
    main()