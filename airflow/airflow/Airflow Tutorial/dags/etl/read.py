import pandas as pd  # type: ignore

def read_data():
    file_path = "/opt/airflow/dags/data/customer_data.csv"
    df = pd.read_csv(file_path)
    print("DATA READ SUCCESS")
    return df.to_json()