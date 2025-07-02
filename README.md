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
- **Conda/Miniconda**: Install miniconda in your environment if you don't have yet conda/miniconda: https://www.anaconda.com/docs/getting-started/miniconda/install
- **Snowflake Account**: This will be provided

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd sf-trading-pipeline-demo
```

### 2. Access this Repository from the Snowflake UI
Login into your Snowflake account and run [`0_create_sf_git_integration.sql`](0_create_sf_git_integration.sql) to create an integration to this github repository. 

Then, go to Projects > Workspaces > Create Workspace From Git Repository

In the popup, enter the following fields:
* Repository URL: copy and paste this repository URL
* Workspace Name: trading-pipeline-demo
* API Integration: GITHUB_INTEGRATION
* Select Public Repository

You'll see this repository inside your Snowflake Workspace!

### 3. Set Up Snowflake Objects

In Snowsight, open the [`1_setup_create_snowflake_objects.sql`](1_setup_create_snowflake_objects.sql) file and run all statements to create the required objects in your account.

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
Update the connection parameters in [`2.local_snowflake_csv_loader.py`](2.local_snowflake_csv_loader.py):
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

Run the python script that loads CSV files from a local directory [`/sample_data`](sample_data/) into your Snowflake tables.
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

## 📚 Additional Resources

- [Snowflake Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Python Graphing Library](https://plotly.com/python/)
- [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector)


