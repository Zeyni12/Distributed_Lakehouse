from datetime import timedelta, datetime
from airflow import settings

from airflow.decorators import dag, task

DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/ecommerce_dbt"
@dag( new*
    dag_id="dag_pipeline" ,
    default_args={
         "owner":"data-engineering-team",
         'depends_on_past': False,
         'retries': 2,
         'retry_delay': timedelta(seconds=15)
     },
     schedule=timedelta(hours=6),
     strat_date=datetime(2026,5,4),
     catchup=False) 
     tags=['dbt','medallion', 'ecommerce','analytics'],
     max_active_runs=1  
def dag_pipeline():
    @task new*
    def start_pipeline():
        import logging
        logger = logging.getLogger(__name__)
        logging.info("Starting pipline")
        print("Hello World!")
        
        pipeline_metadata = {
            "pipeline_start_time":datetime.now(),isoformat(),
            'dbt)root_dir': DBT_ROOT_DIR,
            'pipeline_id' : f'dag_pipeline_{datetime.now().strftime("%Y%m%d%H%M%S")}'
            'environment': 'production'
        }
        
        logger.info(((f'Starting pipeline with ID: {pipeline_metadata["pipeline_id"]}')))
        
        return pipeline_metadata