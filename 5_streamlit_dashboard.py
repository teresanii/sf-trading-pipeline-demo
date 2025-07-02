#!/usr/bin/env python3
"""
Crypto Broker Analytics - Streamlit Dashboard

Interactive dashboard for visualizing crypto trading data from Snowflake.
Displays real-time analytics, trading patterns, user insights, and market overview.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Crypto Broker Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
.main-header {
    font-size: 3rem;
    color: #1f77b4;
    text-align: center;
    margin-bottom: 2rem;
}
.metric-container {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
}
.sidebar-content {
    background-color: #ffffff;
}
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_snowflake_connection():
    """Create and return Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'CRYPTO_ANALYTICS'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

@st.cache_data(ttl=300)
def load_trading_metrics():
    """Load daily trading metrics from Snowflake"""
    conn = get_snowflake_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT * FROM ANALYTICS.DAILY_TRADING_METRICS
            ORDER BY trade_date DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading trading metrics: {str(e)}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_top_assets():
    """Load top performing assets"""
    conn = get_snowflake_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT * FROM ANALYTICS.TOP_PERFORMING_ASSETS
            LIMIT 20
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading top assets: {str(e)}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_user_summary():
    """Load user trading summary"""
    conn = get_snowflake_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT * FROM ANALYTICS.USER_TRADING_SUMMARY
            ORDER BY total_volume DESC
            LIMIT 50
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading user summary: {str(e)}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_trading_patterns():
    """Load trading patterns by hour and exchange"""
    conn = get_snowflake_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT 
                trade_hour,
                exchange,
                symbol,
                SUM(total_trades) as trades,
                SUM(total_notional) as volume
            FROM ANALYTICS.DAILY_TRADING_METRICS
            GROUP BY trade_hour, exchange, symbol
            ORDER BY trade_hour, volume DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading trading patterns: {str(e)}")
        conn.close()
        return pd.DataFrame()

def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">🚀 Crypto Broker Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar for filters and controls
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        # Refresh button
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        
        # Date range selector (placeholder for when we have more data)
        st.subheader("📅 Filters")
        show_all_data = st.checkbox("Show All Data", value=True)
        
        # Asset filter
        st.subheader("💰 Asset Selection")
        selected_assets = st.multiselect(
            "Select Cryptocurrencies",
            options=['BTC-USD', 'ETH-USD', 'ADA-USD', 'DOT-USD', 'SOL-USD', 'AVAX-USD'],
            default=['BTC-USD', 'ETH-USD', 'SOL-USD']
        )
        
        st.markdown("---")
        st.markdown("### 🔗 Data Pipeline Status")
        st.success("✅ Connected to Snowflake")
        st.info("🔄 Dynamic Tables Active")
        st.info("📊 Real-time Processing")
        
    # Load data
    with st.spinner("Loading data from Snowflake..."):
        trading_metrics = load_trading_metrics()
        top_assets = load_top_assets()
        user_summary = load_user_summary()
        trading_patterns = load_trading_patterns()
    
    if trading_metrics.empty:
        st.error("No data available. Please check your Snowflake connection and run the data loader first.")
        return
    
    # Key Performance Indicators
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_trades = trading_metrics['TOTAL_TRADES'].sum()
        st.metric("Total Trades", f"{total_trades:,}", delta="↗️")
    
    with col2:
        total_volume = trading_metrics['TOTAL_NOTIONAL'].sum()
        st.metric("Total Volume", f"${total_volume:,.0f}", delta="📈")
    
    with col3:
        unique_traders = trading_metrics['UNIQUE_TRADERS'].sum()
        st.metric("Active Traders", f"{unique_traders:,}", delta="👥")
    
    with col4:
        avg_trade_size = trading_metrics['TOTAL_NOTIONAL'].sum() / trading_metrics['TOTAL_TRADES'].sum()
        st.metric("Avg Trade Size", f"${avg_trade_size:,.0f}", delta="💰")
    
    st.markdown("---")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Market Overview", "👥 User Analytics", "⏰ Trading Patterns", "🔍 Asset Deep Dive"])
    
    with tab1:
        st.header("🏆 Top Performing Assets")
        
        if not top_assets.empty:
            # Volume chart
            fig_volume = px.bar(
                top_assets.head(10),
                x='SYMBOL',
                y='TOTAL_VOLUME',
                title="Trading Volume by Asset",
                color='TOTAL_VOLUME',
                color_continuous_scale='viridis'
            )
            fig_volume.update_layout(height=400)
            st.plotly_chart(fig_volume, use_container_width=True)
            
            # Assets table
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("💹 Volume Leaders")
                volume_df = top_assets[['SYMBOL', 'TOTAL_VOLUME', 'TOTAL_TRADES']].head(10)
                volume_df['TOTAL_VOLUME'] = volume_df['TOTAL_VOLUME'].apply(lambda x: f"${x:,.0f}")
                st.dataframe(volume_df, use_container_width=True)
            
            with col2:
                st.subheader("📈 Price Overview")
                price_df = top_assets[['SYMBOL', 'AVG_PRICE', 'HIGH_PRICE', 'LOW_PRICE']].head(10)
                for col in ['AVG_PRICE', 'HIGH_PRICE', 'LOW_PRICE']:
                    price_df[col] = price_df[col].apply(lambda x: f"${x:,.2f}")
                st.dataframe(price_df, use_container_width=True)
    
    with tab2:
        st.header("👥 User Trading Analytics")
        
        if not user_summary.empty:
            # User volume distribution
            fig_users = px.histogram(
                user_summary,
                x='TOTAL_VOLUME',
                nbins=20,
                title="Distribution of User Trading Volumes",
                labels={'TOTAL_VOLUME': 'Trading Volume ($)', 'count': 'Number of Users'}
            )
            fig_users.update_layout(height=400)
            st.plotly_chart(fig_users, use_container_width=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Top traders
                st.subheader("🏆 Top Traders by Volume")
                top_traders = user_summary.head(10)[['FULL_NAME', 'TIER', 'TOTAL_VOLUME', 'TOTAL_TRADES']]
                top_traders['TOTAL_VOLUME'] = top_traders['TOTAL_VOLUME'].apply(lambda x: f"${x:,.0f}")
                st.dataframe(top_traders, use_container_width=True)
            
            with col2:
                # User tier distribution
                st.subheader("💎 User Tier Distribution")
                tier_counts = user_summary['TIER'].value_counts()
                fig_tier = px.pie(
                    values=tier_counts.values,
                    names=tier_counts.index,
                    title="Users by Tier"
                )
                st.plotly_chart(fig_tier, use_container_width=True)
            
            # Country analysis
            if 'COUNTRY' in user_summary.columns:
                st.subheader("🌍 Trading by Country")
                country_volume = user_summary.groupby('COUNTRY')['TOTAL_VOLUME'].sum().sort_values(ascending=False)
                fig_country = px.bar(
                    x=country_volume.index,
                    y=country_volume.values,
                    title="Trading Volume by Country"
                )
                fig_country.update_layout(height=400)
                st.plotly_chart(fig_country, use_container_width=True)
    
    with tab3:
        st.header("⏰ Trading Patterns & Timing")
        
        if not trading_patterns.empty:
            # Trading activity by hour
            hourly_data = trading_patterns.groupby('TRADE_HOUR').agg({
                'TRADES': 'sum',
                'VOLUME': 'sum'
            }).reset_index()
            
            fig_hourly = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Trading Volume by Hour', 'Number of Trades by Hour'),
                vertical_spacing=0.1
            )
            
            fig_hourly.add_trace(
                go.Bar(x=hourly_data['TRADE_HOUR'], y=hourly_data['VOLUME'], name='Volume'),
                row=1, col=1
            )
            
            fig_hourly.add_trace(
                go.Bar(x=hourly_data['TRADE_HOUR'], y=hourly_data['TRADES'], name='Trades', marker_color='orange'),
                row=2, col=1
            )
            
            fig_hourly.update_layout(height=600, showlegend=False)
            fig_hourly.update_xaxes(title_text="Hour of Day", row=2, col=1)
            fig_hourly.update_yaxes(title_text="Volume ($)", row=1, col=1)
            fig_hourly.update_yaxes(title_text="Number of Trades", row=2, col=1)
            
            st.plotly_chart(fig_hourly, use_container_width=True)
            
            # Exchange comparison
            st.subheader("🏢 Exchange Performance")
            exchange_data = trading_patterns.groupby('EXCHANGE').agg({
                'TRADES': 'sum',
                'VOLUME': 'sum'
            }).reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_ex_vol = px.pie(
                    exchange_data,
                    values='VOLUME',
                    names='EXCHANGE',
                    title="Volume Distribution by Exchange"
                )
                st.plotly_chart(fig_ex_vol, use_container_width=True)
            
            with col2:
                fig_ex_trades = px.pie(
                    exchange_data,
                    values='TRADES',
                    names='EXCHANGE',
                    title="Trade Count by Exchange"
                )
                st.plotly_chart(fig_ex_trades, use_container_width=True)
    
    with tab4:
        st.header("🔍 Asset Deep Dive")
        
        if selected_assets and not trading_metrics.empty:
            # Filter data for selected assets
            filtered_metrics = trading_metrics[trading_metrics['SYMBOL'].isin(selected_assets)]
            
            if not filtered_metrics.empty:
                # Price trends
                st.subheader("📈 Price Trends")
                fig_prices = px.line(
                    filtered_metrics,
                    x='TRADE_DATE',
                    y='AVG_PRICE',
                    color='SYMBOL',
                    title="Average Price Trends Over Time"
                )
                fig_prices.update_layout(height=400)
                st.plotly_chart(fig_prices, use_container_width=True)
                
                # Volume trends
                st.subheader("📊 Volume Analysis")
                fig_volume_trend = px.bar(
                    filtered_metrics,
                    x='TRADE_DATE',
                    y='TOTAL_NOTIONAL',
                    color='SYMBOL',
                    title="Trading Volume Over Time"
                )
                fig_volume_trend.update_layout(height=400)
                st.plotly_chart(fig_volume_trend, use_container_width=True)
                
                # Detailed metrics table
                st.subheader("📋 Detailed Metrics")
                detailed_df = filtered_metrics[['TRADE_DATE', 'SYMBOL', 'TOTAL_TRADES', 'TOTAL_NOTIONAL', 'AVG_PRICE', 'VWAP']]
                detailed_df = detailed_df.sort_values(['TRADE_DATE', 'TOTAL_NOTIONAL'], ascending=[False, False])
                
                # Format currency columns
                for col in ['TOTAL_NOTIONAL', 'AVG_PRICE', 'VWAP']:
                    detailed_df[col] = detailed_df[col].apply(lambda x: f"${x:,.2f}")
                
                st.dataframe(detailed_df, use_container_width=True)
            else:
                st.warning("No data available for the selected assets.")
        else:
            st.info("Please select at least one asset from the sidebar to view detailed analysis.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        🚀 Crypto Broker Analytics Dashboard | Powered by Snowflake Dynamic Tables & Streamlit<br>
        📊 Real-time data processing | 🔄 Auto-refreshing every 5 minutes
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 