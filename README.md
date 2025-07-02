# 🚀 Trading Data Pipeline Demo

A demonstration using with **Snowflake**, **Python**, and **Streamlit** that shows how to load data into Snowflake from a local filesystem, process and transform this data using dynamic and creates interactive dashboards in Streamlit.

## 📊 What This Demo Does

- **📥 Data Ingestion**: Load CSV files using Python containing crypto trades, user profiles, and order book data into Snowflake
- **🔄 Incremental Data Transformation**: Use Snowflake Dynamic Tables for incremental data transformation and aggregation  
- **📈 Interactive Analytics**: Visualize trading patterns, user behavior, and market insights through a Streamlit dashboard

### Data Flow
1. **Raw Layer**: CSV files uploaded to Snowflake staging area
2. **Staging Layer**: Data cleaning and standardization
3. **Analytics Layer**: Aggregated metrics via Dynamic Tables
4. **Visualization Layer**: Interactive Streamlit dashboard

## 📋 Requirements

### System Requirements
- **Python**: 3.9 or higher
- **Conda/Miniconda**: For environment management. Instructions here: https://docs.snowflake.com/en/developer-guide/snowpark/python/setup
- **Snowflake Account**: This will be provided

### Python Dependencies
```
snowflake-connector-python==3.6.0
```

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd sf-trading-analytics-de
```

### 2. Access this Repository from the Snowflake UI
Login into your Snowflake account and run `0_create_sf_git_integration.sql` to create an integration to this github repository. 

Then, go to Projects > Workspaces > Create Workspace From Git Repository

In the popup, enter the following fields:
* Repository URL: copy and paste this repository URL
* Workspace Name: trading-pipeline-demo
* API Integration: GITHUB_INTEGRATION
* Select Public Repository

You'll see this repository inside your Snowflake Workspace!

### 3. Set Up Snowflake Objects

In Snowsight, open the `1_setup_create_snowflake_objects.sql` file and run all statements to create the required objects in your account.

### 4. Set Up Local Environment

Now, go back to your terminal and setup your local environment.

```bash
# Create conda environment
conda create -n trading-analytics python=3.9 -y
conda activate trading-analytics

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Snowflake Connection

#### Edit Python Files Directly
Update the connection parameters in `2.local_snowflake_csv_loader.py`:
```python
conn = snowflake.connector.connect(
    user='YOUR_SNOWFLAKE_USER',
    password='YOUR_SNOWFLAKE_PASSWORD', 
    account='YOUR_SNOWFLAKE_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='CRYPTO_ANALYTICS',
    schema='RAW_DATA'
)
```

### 5. Load Sample Data

Run the python script that loads CSV files from a local directory (`/sample_data`) into your Snowflake tables.
```bash
python 2_local_snowflake_csv_loader.py
```

### 6. Run Data Pipeline

Back to your Snowflake account, open 

```bash
# Run the notebook in your Snowflake UI
jupyter notebook data_pipeline.ipynb
# Execute all cells to create Dynamic Tables and Analytics Views
```

### 7. Launch Dashboard
```bash
streamlit run streamlit_dashboard.py
```

## 📁 Project Structure

```
Trading_Analytics_HOL/
├── 📄 README.md                          # This file
├── 🔧 requirements.txt                   # Python dependencies
├── 🗃️ 1.setup_create_snowflake_objects.sql  # Snowflake database setup
├── 🐍 2.local_snowflake_csv_loader.py    # Data loading utility
├── 📊 streamlit_dashboard.py             # Interactive dashboard
├── 📓 data_pipeline.ipynb                # Data pipeline notebook
├── 🧪 test_setup.py                      # Setup validation tests
└── 📂 sample_data/                       # Sample CSV files
    ├── crypto_trades_batch1.csv          # Trading data (batch 1)
    ├── crypto_trades_batch2.csv          # Trading data (batch 2)  
    ├── user_profiles.csv                 # User account data
    └── order_book_data.csv               # Market depth data
```

## 💾 Data Schema

### 👤 User Profiles
- User demographics, account tiers, KYC status
- Fields: `user_id`, `email`, `tier`, `country`, `kyc_status`

### 📈 Crypto Trades  
- Individual trade transactions across exchanges
- Fields: `trade_id`, `user_id`, `symbol`, `side`, `quantity`, `price`, `exchange`

### 📊 Order Book Data
- Market depth and liquidity information
- Fields: `timestamp`, `symbol`, `side`, `price`, `quantity`, `level_num`

## 🎯 Use Cases & Analytics

### Trading Analytics
- **Volume Analysis**: Track trading volumes by asset, exchange, and time period
- **Price Analytics**: Monitor price movements, VWAP, and volatility
- **User Behavior**: Analyze trading patterns by user segment and geography

### Business Intelligence
- **Performance Metrics**: Daily/hourly trading summaries and KPIs
- **Asset Rankings**: Top performing cryptocurrencies by volume and trades
- **User Segmentation**: Revenue and activity analysis by user tier

### Operational Insights
- **Exchange Comparison**: Compare liquidity and activity across platforms
- **Time-based Patterns**: Identify peak trading hours and seasonal trends
- **Risk Analytics**: Monitor large trades and unusual activity patterns

## 🔧 Configuration Options

### Snowflake Dynamic Tables
Modify `TARGET_LAG` in the data pipeline for different refresh frequencies:
```sql
TARGET_LAG = '1 minute'  -- Near real-time
TARGET_LAG = '5 minutes' -- Standard refresh
TARGET_LAG = '1 hour'    -- Batch processing
```

### Dashboard Customization
- Update asset filters in `streamlit_dashboard.py`
- Modify visualization themes and layouts
- Add custom metrics and KPIs

## 🚨 Troubleshooting

### Common Issues

**Connection Errors**
- Verify Snowflake credentials and network connectivity
- Check warehouse is running and accessible
- Ensure IP whitelisting if required

**Data Loading Issues**
- Confirm CSV files exist in `sample_data/` directory
- Verify Snowflake objects were created successfully
- Check file format matches expected schema

**Dashboard Problems**
- Ensure all required Python packages are installed
- Verify environment variables are set correctly
- Check Streamlit port (default: 8501) is available

### Performance Optimization
- Use appropriate Snowflake warehouse size for your data volume
- Adjust Dynamic Table refresh frequency based on requirements
- Enable result caching in Streamlit for better performance

## 🧪 Testing

Run the setup validation tests:
```bash
python test_setup.py
```

This will verify:
- ✅ All required files are present
- ✅ Python syntax is valid
- ✅ Dependencies can be imported
- ✅ Configuration is complete

## 🎓 Learning Objectives

This hands-on lab demonstrates:
- **Modern Data Engineering**: ELT patterns with cloud data warehouses
- **Real-time Analytics**: Dynamic tables and incremental processing  
- **Data Visualization**: Interactive dashboards with Python
- **Schema Evolution**: Handling changing data structures
- **Cloud Integration**: Snowflake ecosystem and connectors

## 📚 Additional Resources

- [Snowflake Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Python Graphing Library](https://plotly.com/python/)
- [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is provided as an educational resource. See individual component licenses for specific terms.

## 💬 Support

For questions and issues:
- 📧 Create an issue in this repository
- 💡 Check the troubleshooting section above
- 📖 Review Snowflake and Streamlit documentation

---

🎉 **Happy Analyzing!** Build amazing crypto trading analytics with this hands-on lab.