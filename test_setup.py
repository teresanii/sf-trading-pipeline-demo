#!/usr/bin/env python3
"""Simple test to validate the crypto broker analytics setup"""

import os
import sys

def test_files_exist():
    """Test that all required files exist"""
    required_files = [
        'data_loader.py',
        'streamlit_dashboard.py', 
        'data_pipeline.ipynb',
        'requirements.txt',
        'README.md',
        'setup.py'
    ]
    
    sample_files = [
        'sample_data/crypto_trades_batch1.csv',
        'sample_data/crypto_trades_batch2.csv', 
        'sample_data/user_profiles.csv'
    ]
    
    print("🧪 Testing Crypto Broker Analytics Setup")
    print("=" * 50)
    
    # Test main files
    print("\n📁 Main Project Files:")
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
            return False
    
    # Test sample data files  
    print("\n📊 Sample Data Files:")
    for file in sample_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
            return False
    
    return True

def test_python_syntax():
    """Test that Python files have valid syntax"""
    python_files = ['data_loader.py', 'streamlit_dashboard.py', 'setup.py']
    
    print("\n🐍 Python Syntax Check:")
    for file in python_files:
        try:
            with open(file, 'r') as f:
                compile(f.read(), file, 'exec')
            print(f"✅ {file}")
        except Exception as e:
            print(f"❌ {file}: {e}")
            return False
    
    return True

def main():
    """Run all tests"""
    if test_files_exist() and test_python_syntax():
        print("\n🎉 All tests passed!")
        print("\n📋 Next steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Configure Snowflake: Create .env file with credentials")
        print("3. Load data: python data_loader.py")
        print("4. Run pipeline: jupyter notebook data_pipeline.ipynb")
        print("5. Launch dashboard: streamlit run streamlit_dashboard.py")
        return True
    else:
        print("\n❌ Some tests failed. Please check the setup.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 