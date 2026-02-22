# import streamlit as st
# import pandas as pd
# import time

# st.title("Real-Time Finance Dashboard")
# placeholder = st.empty()

# while True:
#     # In a real scenario, you'd read from Kafka 'aggregates' topic 
#     # For the demo, let's assume you're reading the output file/db
#     try:
#         # Example: Mocking the data flow for the UI layout
#         df = pd.read_csv("latest_aggregates.csv") 
#         with placeholder.container():
#             st.metric("Current Avg Price", f"${df['avg_price'].iloc[-1]}")
#             st.line_chart(df.set_index('window')['avg_price'])
#             st.bar_chart(df['max_price'])
#     except:
#         st.write("Waiting for data...")
    
#     time.sleep(5)

import streamlit as st
import pandas as pd
import time
import plotly.express as px
import glob
import os

st.set_page_config(page_title="Real-Time Stock Dashboard", layout="wide")

st.title("📈 Alpha Vantage Real-Time Analytics")
st.write("Business Scenario: Monitoring Stock Volatility")

# Create placeholders for real-time updates
placeholder = st.empty()

while True:
    try:
        # In a student project, reading from the Spark output Parquet/CSV 
        # is the most stable way to feed Streamlit

        # Get the latest CSV file in the folder
        # list_of_files = glob.glob('data/aggregates/*.csv') 
        # latest_file = max(list_of_files, key=os.path.getctime)
        # df = pd.read_csv(latest_file)
        # df = pd.read_csv("data/aggregates.csv") 
        df = pd.read_parquet("data/aggregates") # If using Parquet output from Spark
        
        with placeholder.container():
            # KPI Indicators
            col1, col2 = st.columns(2)
            latest_price = df['avg_price'].iloc[-1]
            col1.metric("Average Price (Last Window)", f"${latest_price:.2f}")
            col2.metric("Max Volatility", f"${df['max_price'].max():.2f}")

            # Visualization 1: Time Series
            st.subheader("Price Trend (5-Minute Windows)")
            fig_line = px.line(df, x='window', y='avg_price', title="Rolling Average Price")
            st.plotly_chart(fig_line, use_container_width=True)

            # Visualization 2: Bar Chart
            st.subheader("Max Price per Entity")
            fig_bar = px.bar(df, x='symbol', y='max_price', color='symbol')
            st.plotly_chart(fig_bar, use_container_width=True)

    except Exception as e:
        st.info("Waiting for Spark to write the first batch of aggregates...")
    
    time.sleep(5) # Refresh every 5 seconds

# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import time
# import os

# st.set_page_config(page_title="Alpha Vantage Live", layout="wide")

# # 1. Provide the exact path to your aggregates folder
# DATA_PATH = "data/aggregates"

# st.title("📊 Real-Time Market Analysis")

# placeholder = st.empty()

# while True:
#     # Check if the directory exists and has files
#     if os.path.exists(DATA_PATH) and len(os.listdir(DATA_PATH)) > 1:
#         try:
#             # Read the entire Parquet directory
#             df = pd.read_parquet(DATA_PATH, engine='pyarrow')
            
#             if not df.empty:
#                 # IMPORTANT: Since we use 'append' mode, we must keep only the 
#                 # LATEST record for each window/symbol pair
#                 df = df.sort_values('window', ascending=False)
#                 df = df.drop_duplicates(subset=['window', 'symbol'])
                
#                 with placeholder.container():
#                     # KPI: Current Price
#                     latest_val = df.iloc[0]['avg_price']
#                     st.metric("Latest Avg Price", f"${latest_val:.2f}")

#                     # Visualization 1: Time Series
#                     fig = px.line(df, x='window', y='avg_price', color='symbol', title="Price over Time")
#                     st.plotly_chart(fig, use_container_width=True)
                    
#                     # Show raw data for debugging
#                     st.write("Current Data in Spark State:", df)
#         except Exception as e:
#             st.warning(f"Engine is updating files... {e}")
#     else:
#         st.info("Waiting for Spark to commit the first window to disk...")
    
#     time.sleep(5)