#!/bin/bash

# Setup script for Trading Analytics HOL
# This script creates a conda environment, installs dependencies, and runs the data loader

set -e  # Exit on any error

echo "🚀 Setting up Trading Analytics HOL environment..."

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
conda env list | grep -q "^$ENV_NAME " && {
    echo "🗑️  Removing existing environment..."
    conda env remove -n $ENV_NAME -y
}

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
echo "📝 IMPORTANT: Before running the script, you need to update your Snowflake credentials!"
echo "Please edit 2.local_snowflake_csv_loader.py and replace the following placeholders:"
echo "  - YOUR_USER"
echo "  - YOUR_PASSWORD" 
echo "  - YOUR_ACCOUNT"
echo "  - YOUR_WAREHOUSE"
echo ""

# Ask user if they want to proceed
read -p "Have you updated your Snowflake credentials? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🏃 Running the data loader script..."
    python 2.local_snowflake_csv_loader.py
    echo "✅ Script execution completed!"
else
    echo "ℹ️  Please update your credentials in 2.local_snowflake_csv_loader.py"
    echo "Then run: conda activate $ENV_NAME && python 2.local_snowflake_csv_loader.py"
fi

echo ""
echo "🎉 Setup complete! Your conda environment '$ENV_NAME' is ready to use."
echo "To activate it manually: conda activate $ENV_NAME" 