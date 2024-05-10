import mlflow
from mlflow import MlflowClient
mlflow.set_tracking_uri("http://spark-master:5005")

# run_id = mlflow.search_runs(
#     filter_string="tags.mlflow.runName = 'traveling-loon-851' ", \
#     search_all_experiments=True
# ).loc[0,'run_id']
# print(run_id)

client = MlflowClient()
run_id = client.search_runs( 
    experiment_ids="166249389057750071",\
    filter_string="tags.mlflow.runName = 'detector_region_2_20240501151253' "
)[0].info.run_id
print(run_id)