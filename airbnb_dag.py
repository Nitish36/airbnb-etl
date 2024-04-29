from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airbnb_mod import generate_data, write_df

with DAG(
        dag_id="airbnb_end_to_end",
        start_date=datetime(2023, 9, 4),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
) as dag:
    generate_data_task = PythonOperator(
        task_id="generate_data_task",
        python_callable=generate_data,
        provide_context=True
    )
    task_try = PythonOperator(
        task_id="task_try",
        python_callable=write_df,
        provide_context=True
    )

    # write_task = PythonOperator(
    #     task_id="write_task",
    #     python_callable=write_df,
    #     provide_context=True
    # )
    generate_data_task >> task_try