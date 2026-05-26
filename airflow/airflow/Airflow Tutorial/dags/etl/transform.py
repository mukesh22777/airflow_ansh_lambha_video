import pandas as pd  # type: ignore

def transform_data(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='read_task')

    df = pd.read_json(data)

    # Example transformation
    df.columns = [col.upper() for col in df.columns]
    df = df.dropna()

    print("TRANSFORMATION DONE")
    return df.to_json()