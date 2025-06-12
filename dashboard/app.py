import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "mydatabase")


def connect_to_mongodb():
    """Connect to MongoDB and return the database."""
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


def load_data():
    """Load data from MongoDB collections."""
    db = connect_to_mongodb()

    # Load customer bills
    bills = list(db.customer_bills.find({}))
    bills_df = pd.DataFrame(bills)

    # Load customers
    customers = list(db.customers.find({}))
    customers_df = pd.DataFrame(customers)

    # Load invalid records
    invalid_records = list(db.invalid_records.find({}))
    invalid_df = pd.DataFrame(invalid_records)

    return bills_df, customers_df, invalid_df


def main():
    st.set_page_config(page_title="Telecom Dashboard", page_icon="📊", layout="wide")

    # Load data
    bills_df, customers_df, invalid_df = load_data()

    # Sidebar
    st.sidebar.title("Telecom Dashboard")
    st.sidebar.info("Real-time analytics and insights")

    # Main content
    st.title("📊 Telecom Analytics Dashboard")

    # Overview Section
    st.markdown("### 📈 Overview")
    overview_col1, overview_col2 = st.columns([2, 1])

    with overview_col1:
        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Customers",
                len(customers_df),
                delta=f"{len(customers_df) - len(customers_df[customers_df['subscriptionType'] == 'prepaid'])} postpaid",
            )

        with col2:
            total_revenue = bills_df["total_charge"].sum()
            st.metric(
                "Total Revenue",
                f"{total_revenue:,.2f} DH",
                delta=f"{total_revenue/len(customers_df):,.2f} DH per customer",
            )

        with col3:
            avg_voice = bills_df["voice_usage"].mean()
            st.metric(
                "Avg Voice Usage",
                f"{avg_voice:,.0f} min",
                delta=f"{avg_voice - bills_df['voice_usage'].median():,.0f} min vs median",
            )

        with col4:
            avg_data = bills_df["data_usage"].mean()
            st.metric(
                "Avg Data Usage",
                f"{avg_data:,.2f} MB",
                delta=f"{avg_data - bills_df['data_usage'].median():,.2f} MB vs median",
            )

    with overview_col2:
        # Subscription Type Distribution
        sub_type_counts = customers_df["subscriptionType"].value_counts()
        fig_sub = px.pie(
            values=sub_type_counts.values,
            names=sub_type_counts.index,
            title="Subscription Type Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3,
            hole=0.4,
        )
        fig_sub.update_layout(
            showlegend=True,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5
            ),
        )
        st.plotly_chart(fig_sub, use_container_width=True)

    # Rate Plans Section
    st.markdown("### 📱 Rate Plans")
    rate_plan_col1, rate_plan_col2 = st.columns([1, 2])

    with rate_plan_col1:
        # Rate Plan Distribution Donut Chart
        rate_plan_counts = customers_df["ratePlanId"].value_counts()
        fig_rate_plans = px.pie(
            values=rate_plan_counts.values,
            names=[f"Plan {plan}" for plan in rate_plan_counts.index],
            title="Rate Plan Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3,
            hole=0.6,
        )
        fig_rate_plans.update_layout(
            showlegend=True,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5
            ),
        )
        st.plotly_chart(fig_rate_plans, use_container_width=True)

    with rate_plan_col2:
        # Rate Plan Metrics
        cols = st.columns(len(rate_plan_counts))
        for idx, (rate_plan, count) in enumerate(rate_plan_counts.items()):
            with cols[idx]:
                # Calculate average revenue for this plan
                plan_customers = customers_df[customers_df["ratePlanId"] == rate_plan][
                    "_id"
                ]
                plan_revenue = bills_df[bills_df["customer_id"].isin(plan_customers)][
                    "total_charge"
                ].mean()

                st.metric(
                    f"Rate Plan {rate_plan}",
                    f"{count} customers",
                    f"Avg Revenue: {plan_revenue:,.2f} DH",
                )

    # Invalid Records Analysis
    st.markdown("### ⚠️ Invalid Records Analysis")
    invalid_col1, invalid_col2 = st.columns(2)

    with invalid_col1:
        # Calculate lost usage
        lost_voice = (
            len(invalid_df[invalid_df["record_type"] == 1])
            if not invalid_df.empty
            else 0
        )
        lost_sms = (
            len(invalid_df[invalid_df["record_type"] == 2])
            if not invalid_df.empty
            else 0
        )
        lost_data = (
            len(invalid_df[invalid_df["record_type"] == 3])
            if not invalid_df.empty
            else 0
        )

        # Create metrics for lost usage
        st.metric(
            "Invalid Voice Records",
            f"{lost_voice:,.0f}",
            (
                f"{lost_voice/len(bills_df)*100:.1f}% of total records"
                if lost_voice > 0
                else "0% of total"
            ),
        )
        st.metric(
            "Invalid SMS Records",
            f"{lost_sms:,.0f}",
            (
                f"{lost_sms/len(bills_df)*100:.1f}% of total records"
                if lost_sms > 0
                else "0% of total"
            ),
        )
        st.metric(
            "Invalid Data Records",
            f"{lost_data:,.0f}",
            (
                f"{lost_data/len(bills_df)*100:.1f}% of total records"
                if lost_data > 0
                else "0% of total"
            ),
        )

    with invalid_col2:
        # Invalid records by type
        if not invalid_df.empty:
            invalid_by_type = pd.DataFrame(
                {
                    "Type": ["Voice", "SMS", "Data"],
                    "Count": [
                        len(invalid_df[invalid_df["record_type"] == 1]),
                        len(invalid_df[invalid_df["record_type"] == 2]),
                        len(invalid_df[invalid_df["record_type"] == 3]),
                    ],
                }
            )

            fig_invalid = px.bar(
                invalid_by_type,
                x="Type",
                y="Count",
                title="Invalid Records by Type",
                color="Type",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            st.plotly_chart(fig_invalid, use_container_width=True)
        else:
            st.info("No invalid records found")

    # Usage Analysis Section
    st.markdown("### 📊 Usage Analysis")
    usage_col1, usage_col2 = st.columns(2)

    with usage_col1:
        # Usage Distribution
        usage_data = []
        for _, row in bills_df.iterrows():
            usage_data.extend(
                [
                    {
                        "Service": "Voice",
                        "Usage": row["voice_usage"],
                        "Customer": row["customerName"],
                    },
                    {
                        "Service": "SMS",
                        "Usage": row["sms_usage"],
                        "Customer": row["customerName"],
                    },
                    {
                        "Service": "Data",
                        "Usage": row["data_usage"],
                        "Customer": row["customerName"],
                    },
                ]
            )
        usage_df = pd.DataFrame(usage_data)

        # Create box plot using go.Figure instead of px.box
        fig_usage = go.Figure()
        for service in ["Voice", "SMS", "Data"]:
            service_data = usage_df[usage_df["Service"] == service]["Usage"]
            fig_usage.add_trace(
                go.Box(
                    y=service_data,
                    name=service,
                    boxpoints="outliers",
                    marker_color=px.colors.qualitative.Set3[
                        ["Voice", "SMS", "Data"].index(service)
                    ],
                )
            )

        fig_usage.update_layout(
            title="Usage Distribution by Service",
            xaxis_title="Service",
            yaxis_title="Usage",
            showlegend=True,
        )
        st.plotly_chart(fig_usage, use_container_width=True)

    with usage_col2:
        # Revenue by Service
        revenue_data = pd.DataFrame(
            {
                "Service": ["Voice", "SMS", "Data"],
                "Revenue": [
                    bills_df["voice_charge"].sum(),
                    bills_df["sms_charge"].sum(),
                    bills_df["data_charge"].sum(),
                ],
            }
        )

        fig_revenue = px.bar(
            revenue_data,
            x="Service",
            y="Revenue",
            title="Revenue by Service",
            color="Service",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        st.plotly_chart(fig_revenue, use_container_width=True)

    # Top Customers Section
    st.markdown("### 👥 Top Customers")
    top_customers = bills_df.nlargest(5, "total_charge")
    fig_top = px.bar(
        top_customers,
        x="customerName",
        y="total_charge",
        title="Top 5 Customers by Revenue",
        color="total_charge",
        color_continuous_scale="Viridis",
    )
    fig_top.update_layout(xaxis_title="Customer", yaxis_title="Revenue (DH)")
    st.plotly_chart(fig_top, use_container_width=True)

    # Detailed Data Section
    st.markdown("### 📋 Detailed Customer Data")
    st.dataframe(
        bills_df[
            [
                "customerName",
                "subscriptionType",
                "total_charge",
                "voice_usage",
                "sms_usage",
                "data_usage",
            ]
        ]
        .sort_values("total_charge", ascending=False)
        .style.format(
            {
                "total_charge": "{:,.2f}",
                "voice_usage": "{:,.0f}",
                "sms_usage": "{:,.0f}",
                "data_usage": "{:,.2f}",
            }
        )
    )


if __name__ == "__main__":
    main()
