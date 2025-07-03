"""
Interactive dashboard for visualizing crypto trading data from Snowflake.
Displays real-time analytics, trading patterns, user insights, and market overview.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col as col_, sum as sum_, count
session = get_active_session()

# Page configuration
st.set_page_config(
    page_title="Trading dashboard",
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

def load_trading_metrics():    
    """Load daily trading metrics from Snowflake"""
    return session.table("DAILY_TRADING_METRICS").sort(col_("TRADE_DATE").desc())

def load_top_assets():
    """Load top performing assets"""
    return session.table("TOP_PERFORMING_ASSETS").limit(20)

def load_user_summary():
    """Load user trading summary"""
    return session.table("USER_TRADING_SUMMARY").sort(col_("TOTAL_VOLUME"), ascending=False).limit(50)

def load_trading_patterns():
    """Load trading patterns by hour and exchange"""
    daily_metrics_df = session.table("DAILY_TRADING_METRICS")

    # Group by trade_hour, exchange, symbol and calculate sums
    result_df = daily_metrics_df.group_by(
        col_("TRADE_DATE"),
        col_("EXCHANGE"),
        col_("SYMBOL")
    ).agg(
        sum_(col_("TOTAL_TRADES")).alias("TRADES"),
        sum_(col_("TOTAL_NOTIONAL")).alias("VOLUME")
    ).sort(
        col_("TRADE_DATE"),
        col_("VOLUME").desc()
    )

    return result_df

def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">🚀 Trading Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar for filters and controls
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        # Refresh button
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        
        
        # Asset filter
        st.subheader("💰 Asset Selection")
        selected_assets = st.multiselect(
            "Select Cryptocurrencies",
            options=['BTC/USD', 'ETH/USD', 'ADA/USD', 'DOT/USD', 'SOL/USD', 'AVAX/USD'],
            default=['BTC/USD', 'ETH/USD', 'SOL/USD']
        )
                
    # Load data
    with st.spinner("Loading data from Snowflake..."):
        trading_metrics = load_trading_metrics()
        top_assets = load_top_assets()
        user_summary = load_user_summary()
        trading_patterns = load_trading_patterns()
    
    if trading_metrics.count() == 0:
        st.error("No data available. Please check your Snowflake connection and run the data loader first.")
        return
    
    # Key Performance Indicators
    col1, col2, col3, col4 = st.columns(4)
    
    # Check if we have data and the required columns exist
    if all(col in trading_metrics.columns for col in ['TOTAL_TRADES', 'TOTAL_NOTIONAL', 'UNIQUE_TRADERS']):
        with col1:
            total_trades = trading_metrics.select(sum_(trading_metrics['TOTAL_TRADES'])).collect()[0][0]
            st.metric("Total Trades", f"{total_trades:,}", delta="↗️")
        
        with col2:
            total_volume = float(trading_metrics.select(sum_(trading_metrics['TOTAL_NOTIONAL'])).collect()[0][0])
            st.metric("Total Volume", f"${total_volume:,.0f}", delta="📈")
        
        with col3:
            unique_traders = int(trading_metrics.select(sum_(trading_metrics['UNIQUE_TRADERS'])).collect()[0][0])
            st.metric("Active Traders", f"{unique_traders:,}", delta="👥")
        
        with col4:
            total_notional_sum = float(trading_metrics.select(sum_(trading_metrics['TOTAL_NOTIONAL'])).collect()[0][0])
            total_trades_sum = int(trading_metrics.select(sum_(trading_metrics['UNIQUE_TRADERS'])).collect()[0][0])
            avg_trade_size = total_notional_sum / total_trades_sum if total_trades_sum > 0 else 0
            st.metric("Avg Trade Size", f"${avg_trade_size:,.0f}", delta="💰")
    else:
        # Show placeholder metrics when no data is available
        with col1:
            st.metric("Total Trades", "0", delta="No Data")
        with col2:
            st.metric("Total Volume", "$0", delta="No Data")
        with col3:
            st.metric("Active Traders", "0", delta="No Data")
        with col4:
            st.metric("Avg Trade Size", "$0", delta="No Data")
    
    st.markdown("---")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Market Overview", "👥 User Analytics", "⏰ Trading Patterns", "🔍 Asset Deep Dive"])
    
    with tab1:
        st.header("🏆 Top Performing Assets")
        
        if not top_assets.count() == 0:
            # Volume chart
            fig_volume = px.bar(
                top_assets.limit(10),
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
                volume_df = top_assets[['SYMBOL', 'TOTAL_VOLUME', 'TOTAL_TRADES']].limit(10)
                pandas_volume_df = volume_df.to_pandas()
                pandas_volume_df['TOTAL_VOLUME'] = pd.to_numeric(pandas_volume_df['TOTAL_VOLUME'], errors='coerce').fillna(0)
                pandas_volume_df['TOTAL_VOLUME'] = pandas_volume_df['TOTAL_VOLUME'].apply(lambda x: f"${x:,.0f}")
                
                st.dataframe(volume_df, use_container_width=True)
            
            with col2:
                st.subheader("📈 Price Overview")
                price_df = top_assets[['SYMBOL', 'AVG_PRICE', 'HIGH_PRICE', 'LOW_PRICE']].limit(10)
                for col in ['AVG_PRICE', 'HIGH_PRICE', 'LOW_PRICE']:
                    new_price_df = price_df.to_pandas()
                    new_price_df[col] = pd.to_numeric(new_price_df[col], errors='coerce').fillna(0)
                    new_price_df[col] = new_price_df[col].apply(lambda x: f"${x:,.2f}")
                st.dataframe(price_df, use_container_width=True)
    
    with tab2:
        st.header("👥 User Trading Analytics")
        
        if not user_summary.count() == 0:
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
                top_traders = user_summary.limit(10)[['FULL_NAME', 'TIER', 'TOTAL_VOLUME', 'TOTAL_TRADES']]
                new_top_traders_df = top_traders.to_pandas()
                new_top_traders_df['TOTAL_VOLUME'] = pd.to_numeric(new_top_traders_df['TOTAL_VOLUME'], errors='coerce').fillna(0)
                new_top_traders_df = new_top_traders_df['TOTAL_VOLUME'].apply(lambda x: f"${x:,.0f}")
                
                st.dataframe(top_traders, use_container_width=True)
            
            with col2:
                # User tier distribution
                st.subheader("💎 User Tier Distribution")
                tier_counts = user_summary.group_by('TIER').agg(count("*").alias("COUNT")).to_pandas()
                fig_tier = px.pie(
                    tier_counts,
                    values='COUNT',
                    names=tier_counts.index,
                    title="Users by Tier"
                )
                st.plotly_chart(fig_tier, use_container_width=True)
            
            # Country analysis
            if 'COUNTRY' in user_summary.columns:
                st.subheader("🌍 Trading by Country")
                country_volume = user_summary.group_by(
                    col_("COUNTRY")
                ).agg(
                    sum_(col_("TOTAL_VOLUME")).alias("TOTAL_VOLUME")
                ).sort(
                    col_("TOTAL_VOLUME").desc()
                ).to_pandas()
                fig_country = px.bar(country_volume,
                    x='COUNTRY',
                    y='TOTAL_VOLUME',
                    title="Trading Volume by Country"
                )
                fig_country.update_layout(height=400)
                st.plotly_chart(fig_country, use_container_width=True)


               
       
    with tab3:
        st.header("⏰ Trading Patterns & Timing")
        
        if not trading_patterns.count() == 0:
            # Trading activity by hour
            hourly_data = trading_patterns.group_by('TRADE_DATE').agg(
                sum_('TRADES').alias('TRADES'),  
                sum_('VOLUME').alias('VOLUME')    
            ).to_pandas()

            fig_hourly = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Trading Volume by Hour', 'Number of Trades by Hour'),
                vertical_spacing=0.1
            )
            
            fig_hourly.add_trace(
                go.Bar(x=hourly_data['TRADE_DATE'], y=hourly_data['VOLUME'], name='Volume'),
                row=1, col=1
            )
            
            fig_hourly.add_trace(
                go.Bar(x=hourly_data['TRADE_DATE'], y=hourly_data['TRADES'], name='Trades', marker_color='orange'),
                row=2, col=1
            )
            
            fig_hourly.update_layout(height=600, showlegend=False)
            fig_hourly.update_xaxes(title_text="Hour of Day", row=2, col=1)
            fig_hourly.update_yaxes(title_text="Volume ($)", row=1, col=1)
            fig_hourly.update_yaxes(title_text="Number of Trades", row=2, col=1)
            
            st.plotly_chart(fig_hourly, use_container_width=True)
            
            # Exchange comparison
            st.subheader("🏢 Exchange Performance")
              
            exchange_data = trading_patterns.group_by('EXCHANGE').agg(
                sum_('TRADES').alias('TRADES'),
                sum_('VOLUME').alias('VOLUME')
            ).to_pandas()
            
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
        
        if selected_assets and not trading_metrics.count() == 0:
            # Filter data for selected assets
            filtered_metrics = trading_metrics[trading_metrics['SYMBOL'].isin(selected_assets)]
            
            if not filtered_metrics.count() == 0:
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
                detailed_df = detailed_df.sort(col_('TRADE_DATE').desc(), col_('TOTAL_NOTIONAL'))
                
                # Format currency columns
                for col in ['TOTAL_NOTIONAL', 'AVG_PRICE', 'VWAP']:
                    new_detailed_df = detailed_df.to_pandas()
                    new_detailed_df[col] = pd.to_numeric(new_detailed_df[col], errors='coerce').fillna(0)
                    new_detailed_df[col] = new_detailed_df[col].apply(lambda x: f"${x:,.2f}")


                    # new_price_df = price_df.to_pandas()
                    # new_price_df[col] = pd.to_numeric(new_price_df[col], errors='coerce').fillna(0)
                    # new_price_df[col] = new_price_df[col].apply(lambda x: f"${x:,.2f}")

                    
                
                st.dataframe(detailed_df, use_container_width=True)
            else:
                st.warning("No data available for the selected assets.")
        else:
            st.info("Please select at least one asset from the sidebar to view detailed analysis.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        Powered by Snowflake | 🔄 Auto-refreshing every 5 minutes
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 