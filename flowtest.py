import httpx
from prefect import flow, task, runtime
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import TASK_SOURCE
import time

@flow(log_prints=True, name="Location Windspeed Forecast")
def location_forecast(latitude: float, longitude: float):
    windspeed = get_location_forecast(latitude, longitude)
    print(f"Flow Name is {runtime.flow_run.name}")
    print_windspeed(windspeed)

@task(retries=2)
def get_location_forecast(latitude: float, longitude: float):
    if runtime.task_run.run_count == 1:
        raise Exception("First attempt, raising exception as requested.")
    else:
        res = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "hourly": "windspeed_10m",
            }
        )
    return res.json()["hourly"]["windspeed_10m"][0]

@task
def print_windspeed(windspeed: float):
    print(f"The windspeed is {windspeed} m/s")
    markdown_content = f"""
| Metric       | Value            |
| ------------ | ---------------- |
| Windspeed    | {windspeed} m/s  |
    """
    create_markdown_artifact(
        key="windspeed",
        description="Windspeed Forecast",
        markdown=markdown_content
    )

@task(cache_policy=TASK_SOURCE)
def my_stateful_task():
    print('sleeping')
    time.sleep(10)
    return 42

if __name__ == "__main__":
    location_forecast.serve()