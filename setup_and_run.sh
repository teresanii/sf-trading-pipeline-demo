#!/bin/bash

# 🚀 Trading Analytics HOL - Environment Setup Script
# This script sets up a conda environment and installs all dependencies

set -e  # Exit on any error

echo "🚀 Setting up Trading Analytics HOL environment..."
echo "=================================================="

# Check if conda is installed
if ! command -v conda &> /dev/null; then
    echo "❌ Conda is not installed. Please install Anaconda or Miniconda first."
    echo "Download from: https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi

# Environment name
ENV_NAME="trading-analytics"

echo "📦 Creating conda environment: $ENV_NAME"

# Remove existing environment if it exists
if conda env list | grep -q "^$ENV_NAME "; then
    echo "🗑️  Removing existing environment..."
    conda env remove -n $ENV_NAME -y
fi

# Create new environment with Python 3.9
echo "🔨 Creating new conda environment with Python 3.9..."
conda create -n $ENV_NAME python=3.9 -y

echo "🔧 Activating environment and installing dependencies..."

# Activate environment and install dependencies
source $(conda info --base)/etc/profile.d/conda.sh
conda activate $ENV_NAME

# Install dependencies from requirements.txt
echo "📋 Installing Python packages..."
pip install -r requirements.txt

echo "✅ Environment setup complete!"
echo ""
echo "📝 NEXT STEPS:"
echo "============="
echo ""
echo "1. 🔐 Configure Snowflake Credentials:"
echo "   Option A: Edit 2.local_snowflake_csv_loader.py with your credentials"
echo "   Option B: Create .env file with:"
echo "     SNOWFLAKE_ACCOUNT=your_account"
echo "     SNOWFLAKE_USER=your_username"
echo "     SNOWFLAKE_PASSWORD=your_password"
echo "     SNOWFLAKE_WAREHOUSE=your_warehouse"
echo ""
echo "2. 🗃️  Set up Snowflake objects:"
echo "   Copy and paste 1.setup_create_snowflake_objects.sql into Snowflake Console"
echo ""
echo "3. 📊 Load sample data:"
echo "   conda activate $ENV_NAME"
echo "   python 2.local_snowflake_csv_loader.py"
echo ""
echo "4. 🧪 Run data pipeline:"
echo "   jupyter notebook data_pipeline.ipynb"
echo ""
echo "5. 📈 Launch dashboard:"
echo "   streamlit run streamlit_dashboard.py"
echo ""
echo "🎉 Environment '$ENV_NAME' is ready!"
echo ""

# Ask if user wants to proceed with next steps
read -p "Would you like to proceed with loading data now? (requires Snowflake setup) [y/N]: " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🔍 Checking Snowflake configuration..."
    
    # Check if .env file exists
    if [ -f ".env" ]; then
        echo "✅ Found .env file"
        echo "🏃 Running data loader..."
        python 2.local_snowflake_csv_loader.py
        echo ""
        echo "🎉 Data loading completed!"
        echo ""
        echo "🚀 Ready to launch dashboard:"
        echo "   streamlit run streamlit_dashboard.py"
    else
        echo "⚠️  No .env file found. Please configure your Snowflake credentials first."
        echo "   See the README.md for setup instructions."
    fi
else
    echo "ℹ️  Setup complete! Follow the next steps above when ready."
fi

echo ""
echo "📚 For detailed instructions, see README.md"
echo "🔧 To activate environment later: conda activate $ENV_NAME" 