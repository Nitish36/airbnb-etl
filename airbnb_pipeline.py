from airbnb_mod import write_df
from prefect import flow, task


@task
def json_pipeline():
    write_df()


@flow
def push_to_api():
    print("Pipeline is running")
    json_pipeline()


if __name__ == "__main__":
    push_to_api()
