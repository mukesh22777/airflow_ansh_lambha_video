from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.models import Variable
from airflow.utils.timezone import utc
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime

from include.api_client import call_api
from include.utils import write_json
from include.business_logic import merge_files, calculate_checkout_amount


AUTH_HEADER = Variable.get("API_AUTH_HEADER")
BASE_URL = Variable.get("BASE_API_URL")


@dag(
    dag_id="api_implementation_dag",
    start_date=datetime(2026, 1, 1, tzinfo=utc),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=utc),
    catchup=False,
)
def pipeline():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    @task
    def fetch_login(ds):
        data = call_api(f"{BASE_URL}/loginUsers", ds, ds, AUTH_HEADER)
        path = f"/opt/airflow/output_files/login/{ds}.json"
        write_json(data, path)
        return path

    @task
    def fetch_product(ds):
        data = call_api(f"{BASE_URL}/productUsers", ds, ds, AUTH_HEADER)
        path = f"/opt/airflow/output_files/product/{ds}.json"
        write_json(data, path)
        return path

    @task
    def fetch_checkout(ds):
        data = call_api(f"{BASE_URL}/checkoutUsers", ds, ds, AUTH_HEADER)
        path = f"/opt/airflow/output_files/checkout/{ds}.json"
        write_json(data, path)
        return path

    @task
    def merge_data(login_path, product_path, checkout_path, ds):
        merged_path = f"/opt/airflow/output_files/merged/{ds}.json"
        return merge_files(login_path, product_path, checkout_path, merged_path)


    @task.branch
    def branch_on_amount(checkout_path):
        total = calculate_checkout_amount(checkout_path)
        if total <= 0:
            return "alarming_situation"
        return "merge_data"


    # alarming_situation = EmailOperator(
    #     task_id="alarming_situation",
    #     to=["mverma6250@gmail.com"],
    #     subject="Checkout Amount ZERO",
    #     html_content="""
    #     <h3>Alarming Situation</h3>
    #     <p>Checkout amount is <b>0</b> for {{ ds }}</p>
    #     """
    # )

    alarming_situation = BashOperator(
        task_id="alarming_situation",
        bash_command='echo "Alarming Situation: Checkout amount is 0 for {{ ds }}"',
    )

    # notify_team = EmailOperator(
    #     task_id="notify_team",
    #     to=["mverma6250@gmail.com"],
    #     subject="Daily API Pipeline Success",
    #     html_content="""
    #     <h3>Pipeline Completed</h3>
    #     <p>Merged data generated successfully for {{ ds }}</p>
    #     """
    # )

    notify_team = BashOperator(
        task_id="notify_team",
        bash_command='echo "Daily API Pipeline Success: Merged data generated successfully for {{ ds }}"',
    )


    login = fetch_login()
    product = fetch_product()
    checkout = fetch_checkout()

    decision = branch_on_amount(checkout)

    merged = merge_data(login, product, checkout)

    start >> [login, product, checkout]
    [login, product, checkout] >> decision
    decision >> [alarming_situation, merged]
    merged >> notify_team
    [alarming_situation, notify_team] >> end



dag = pipeline()



