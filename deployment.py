from datetime import timedelta

from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule

from train import main

deployment = Deployment.build_from_flow(
    flow=main,
    name="model_training_validation",
    schedule=IntervalSchedule(interval=timedelta(minutes=5)),
    work_queue_name="ml",
)

if __name__ == "__main__":
    deployment.apply()
