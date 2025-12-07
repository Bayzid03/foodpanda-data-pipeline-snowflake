import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# App Title
st.title("📊 Foodpanda Revenue Dashboard")

# Get Snowflake session
session = get_active_session()

# Utility functions
def format_revenue(revenue):
    return f"₹{revenue:,.0f}"

def highlight_rows(row):
    color = '#f2f2f2' if row.name % 2 == 0 else 'white'
    return ['background-color: {}'.format(color)] * len(row)

def snowpark_to_pandas(snowpark_df, columns):
    return pd.DataFrame(snowpark_df, columns=columns)

# Fetch functions
def fetch_yearly_kpis():
    query = "SELECT * FROM foodpanda_db.enriched.vw_yearly_revenue_kpis ORDER BY year;"
    return session.sql(query).collect()

def fetch_monthly_kpis(year):
    query = f"SELECT month, total_revenue FROM sandbox.enriched.vw_monthly_revenue_kpis WHERE year={year} ORDER BY month;"
    return session.sql(query).collect()

def fetch_top_restaurants(year, month):
    query = f"""
    SELECT restaurant_name, total_revenue, total_orders, avg_revenue_per_order, avg_revenue_per_item, max_order_value
    FROM sandbox.enriched.vw_monthly_revenue_by_restaurant
    WHERE year={year} AND month={month}
    ORDER BY total_revenue DESC LIMIT 10;
    """
    return session.sql(query).collect()

def fetch_weekday_vs_weekend():
    query = "SELECT day_type, total_revenue, total_orders, avg_revenue_per_order, avg_revenue_per_item FROM enriched.vw_weekday_vs_weekend_revenue;"
    return session.sql(query).collect()

def fetch_top5_restaurants():
    query = "SELECT * FROM enriched.vw_top5_restaurants_revenue;"
    return session.sql(query).collect()

# =========================
# Yearly KPIs
# =========================
sf_df = fetch_yearly_kpis()
df = pd.DataFrame(sf_df, columns=['YEAR','TOTAL_REVENUE','TOTAL_ORDERS','AVG_REVENUE_PER_ORDER','AVG_REVENUE_PER_ITEM','MAX_ORDER_VALUE','REVENUE_GROWTH_PCT'])

st.subheader("📈 Aggregate KPIs Across All Years")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Revenue", format_revenue(df['TOTAL_REVENUE'].sum()))
with col2:
    st.metric("Total Orders", f"{df['TOTAL_ORDERS'].sum():,}")
with col3:
    st.metric("Max Order Value", format_revenue(df['MAX_ORDER_VALUE'].max()))

st.divider()

# Year selector
years = sorted(df["YEAR"].unique())
selected_year = st.selectbox("Select Year", years, index=len(years)-1)

year_data = df[df["YEAR"] == selected_year].iloc[0]
st.subheader(f"📊 KPI Scorecard for {selected_year}")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Revenue", format_revenue(year_data["TOTAL_REVENUE"]), delta=f"{year_data['REVENUE_GROWTH_PCT']}% growth")
with col2:
    st.metric("Avg Revenue per Order", format_revenue(year_data["AVG_REVENUE_PER_ORDER"]))
with col3:
    st.metric("Avg Revenue per Item", format_revenue(year_data["AVG_REVENUE_PER_ITEM"]))

st.divider()

# =========================
# Monthly Revenue Trend
# =========================
month_sf_df = fetch_monthly_kpis(selected_year)
month_df = pd.DataFrame(month_sf_df, columns=['Month','Total Monthly Revenue'])

month_mapping = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
month_df['Month'] = month_df['Month'].map(month_mapping)

st.subheader(f"📅 {selected_year} Monthly Revenue Trend")
bar_chart = alt.Chart(month_df).mark_bar(color="#ff5200").encode(
    x=alt.X('Month', sort=list(month_mapping.values())),
    y=alt.Y('Total Monthly Revenue', title='Revenue (₹)'),
    tooltip=['Month','Total Monthly Revenue']
)
st.altair_chart(bar_chart, use_container_width=True)

line_chart = alt.Chart(month_df).mark_line(color="#ff5200", point=True).encode(
    x='Month',
    y='Total Monthly Revenue',
    tooltip=['Month','Total Monthly Revenue']
)
st.altair_chart(line_chart, use_container_width=True)

st.divider()

# =========================
# Weekday vs Weekend Split
# =========================
weekday_sf_df = fetch_weekday_vs_weekend()
weekday_df = pd.DataFrame(weekday_sf_df, columns=['Day Type','Total Revenue','Total Orders','Avg Revenue per Order','Avg Revenue per Item'])

st.subheader("📆 Weekday vs Weekend Revenue Split")
pie_chart = alt.Chart(weekday_df).mark_arc().encode(
    theta='Total Revenue',
    color='Day Type',
    tooltip=['Day Type','Total Revenue','Total Orders']
)
st.altair_chart(pie_chart, use_container_width=True)

st.divider()

# =========================
# Top Restaurants
# =========================
top5_sf_df = fetch_top5_restaurants()
top5_df = pd.DataFrame(top5_sf_df, columns=['Restaurant Name','Total Revenue','Total Orders','Avg Revenue per Order'])

st.subheader("🏆 Top 5 Restaurants by Revenue")
st.dataframe(top5_df.style.apply(highlight_rows, axis=1), hide_index=True)

# Export option
csv = top5_df.to_csv(index=False)
st.download_button("Download Top 5 Restaurants CSV", csv, "top5_restaurants.csv", "text/csv")
