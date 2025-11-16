from airflow import DAG
from airflow.providers.standard.operators.pythonimport PythonOperator
from datetime import datetime

def test_import():
    """Test importing all required libraries"""
    print("\n" + "="*60)
    print("TESTING LIBRARY IMPORTS")
    print("="*60 + "\n")
    
    # Test requests
    try:
        import requests
        print(f"✓ requests {requests.__version__} - SUCCESS")
    except Exception as e:
        print(f"✗ requests - FAILED: {str(e)}")
        raise
    
    # Test configparser
    try:
        import configparser
        print(f"✓ configparser - SUCCESS (standard library)")
    except Exception as e:
        print(f"✗ configparser - FAILED: {str(e)}")
        raise
    
    # Test math.ceil
    try:
        from math import ceil
        print(f"✓ math.ceil - SUCCESS (standard library)")
        test_value = ceil(3.2)
        print(f"  - Test: ceil(3.2) = {test_value}")
    except Exception as e:
        print(f"✗ math.ceil - FAILED: {str(e)}")
        raise
    
    # Test time
    try:
        import time
        print(f"✓ time - SUCCESS (standard library)")
    except Exception as e:
        print(f"✗ time - FAILED: {str(e)}")
        raise
    
    # Test polars
    try:
        import polars as pl
        print(f"✓ polars {pl.__version__} - SUCCESS")
        # Quick test
        df = pl.DataFrame({'test': [1, 2, 3]})
        print(f"  - Created test DataFrame with {len(df)} rows")
    except Exception as e:
        print(f"✗ polars - FAILED: {str(e)}")
        raise
    
    # Test datetime
    try:
        from datetime import datetime
        print(f"✓ datetime - SUCCESS (standard library)")
        print(f"  - Current time: {datetime.now()}")
    except Exception as e:
        print(f"✗ datetime - FAILED: {str(e)}")
        raise
    
    print("\n" + "="*60)
    print("ALL LIBRARIES IMPORTED SUCCESSFULLY!")
    print("="*60 + "\n")
    
    return "All imports successful!"

with DAG(
    "test_custom_libraries", # Dag id
    start_date=datetime(2021, 1 ,1), # start date, the 1st of January 2021 
    schedule='@daily',  # Cron expression, here it is a preset of Airflow, @daily means once every day.
    catchup=False  # Catchup 
) as dag:
    PythonOperator(
        task_id = "test_library",
        python_callable = test_import,
    )