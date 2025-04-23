import streamlit as st
import pandas as pd

# --- Secrets / Config (for local use) ---
DATABRICKS_SERVER_HOSTNAME = st.secrets["DATABRICKS_SERVER_HOSTNAME"]
DATABRICKS_HTTP_PATH = st.secrets["DATABRICKS_HTTP_PATH"]
DATABRICKS_ACCESS_TOKEN = st.secrets["DATABRICKS_ACCESS_TOKEN"]


# --- Databricks SQL query function ---
@st.cache_data
def load_data_from_databricks():
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            query = "SELECT * FROM workspace.default.pvwatts_hourly_silver"
            df = pd.read_sql(query, connection)
    df["date"] = pd.to_datetime(df["date"])
    return df

# --- Load data and continue app logic ---

df = load_data_from_databricks()

# --- Sidebar filters ---
st.sidebar.header("🔍 Filters")
start_date = st.sidebar.date_input("Start date", df["date"].min())
end_date = st.sidebar.date_input("End date", df["date"].max())

filtered_df = df[(df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))]

# --- Metric Highlights ---
st.subheader("📊 Summary Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total AC Output (kWh)", f"{filtered_df['ac'].sum():,.0f}")
col2.metric("Avg POA (W/m²)", f"{filtered_df['poa'].mean():.1f}")
col3.metric("Avg Cell Temp (°C)", f"{filtered_df['tcell'].mean():.1f}")

# --- Time series chart ---
st.subheader("📈 Hourly AC Output Over Time")
fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(filtered_df["date"], filtered_df["ac"], label="AC Output (kWh)", color="orange")
ax.set_xlabel("Date")
ax.set_ylabel("AC Output (kWh)")
ax.grid(True)
st.pyplot(fig)

# --- Data preview ---
st.subheader("🔢 Raw Data Sample")
st.dataframe(filtered_df.head(100))
