# from datetime import timedelta, datetime
# from airflow import settings

# from airflow.decorators import dag, task

# DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/ecommerce_dbt"
# @dag( 
#     dag_id="dag_pipeline" ,
#     default_args={
#          "owner":"data-engineering-team",
#          'depends_on_past': False,
#          'retries': 2,
#          'retry_delay': timedelta(seconds=15)
#      },
#     schedule=timedelta(hours=6),
#     strat_date=datetime(2026,5,4),
#     catchup=False), 
#     tags=['dbt','medallion', 'ecommerce','analytics'],
#     max_active_runs=1  
#     )
# def dag_pipeline():
#     @task 
#     def start_pipeline():
#         import logging
#         logger = logging.getLogger(__name__)
#         logging.info("Starting pipline")
#         print("Hello World!")
        
#         pipeline_metadata = {
#             "pipeline_start_time":datetime.now(),isoformat(),
#             'dbt)root_dir': DBT_ROOT_DIR,
#             'pipeline_id' : f'dag_pipeline_{datetime.now().strftime("%Y%m%d%H%M%S")}'
#             'environment': 'production'
#         }
        
#         logger.info(((f'Starting pipeline with ID: {pipeline_metadata["pipeline_id"]}')))
        
#         return pipeline_metadata
    
#     @task 
#     def seed_bronze(pipeline_metadat):
#         import logging
#         from operators.dbt_operator import DbtOperator
        
#         logger = logging.getLogger(__name__)
#         logger.info('seeding bronze...')
        
#         try:
#             import sqlalchemy
#             from sqlalchemy import text
            
#             engine = sqlalchemy.create_engine('trino://trino@trino-coordinator:8080/iceberg/bronze')
#             with engine.connect() as conn:
#                 result = conn.excute(text("SELECT count(*) as cnt FROM raw_customer_events "))
#                 row_count = result.scalar()
                
#                 if row_count and row_count > 0:
#                     logger.info(f"Bronze already seeded with {row_count} rows, skipping seeding")
#                     return {
#                         'status':'skipped',
#                         'layer':'bronze_seed',
#                         'pipeline_id': pipeline_metadat['pipeline_id'],
#                         'timestamp':datetime.now().isoformat(),
#                         'message':f'Tables already seeded and with {row_count} rows'
#                     }
#                 #logger.info("Bronze seeded successefully")
#         except Exception as e:
#             logger.info(f"Tables do not exist or error occured: {e}, proceeding with seeding")
            
#             operator = DbtOperator(
#                 task_id='seed_bronze_data_internal',
#                 dbt_root_dir=DBT_ROOT_DIR,
#                 dbt_command='seed',
#                 full_refresh=True,
                
#             )
            
#             try:
#                 operator.excute(context={})
#                 return {
#                     'status': 'success',
#                     'layer': 'bronze_seed',
#                     'pipeline_id': pipeline_metadat['pipeline_id'],
#                     'timestamp': datetime.now().isoformat(),
#                 }
#             except Exception as e:
#                 logger.warning(f"Error occured during seeding: {e}")
                
#                 return {
#                     'status': 'success',
#                     'layer': 'bronze_seed',
#                     'pipeline_id': pipeline_metadat['pipeline_id'],
#                     'timestamp': datetime.now().isoformat(),
#                     'warning': str(e)
#                 } 
     
     
#     @task 
#     def transform_bronze_layer(seed_result):
#         import logging
#         from operators.dbt_operator import DbtOperator
#         from airflow import settings 
        
#         logger = logging.getlogger(__name__)
        
#         if seed_result['status'] == 'failed':
#             logger.warning(f"Seeding failed, continuing with transformation...{seed_result.get('warning', 'Unknown error occured')}")
            
#             logger.info("Transforming bronze...")
            
#             operator = DbtOperator(
#                 task_id = 'transforms_bronze_data_internal'
#                 dbt_root_dir=DBT_ROOT_DIR,
#                 dbt_command='run --select tag:bronze'
                
#             )
            
#             try:
#                 operator.excute(context={})
#                 return {
#                     'status': 'success',
#                     'layer': 'bronze_seed',
#                     'pipeline_id': seed_result['pipeline_id'],
#                     'timestamp': datetime.now().isoformat(),
#                     'row_processed': 'check logs for details'
                    
#                 }
#             except Exception as e:
#                 logger.warning(f'Error occured during transformation: {e}') 
#                 raise   
                    
#     @task
#     def validated_bronze_data(bronze_result):
#         import logging
#         logger = logging.getLogger(__name__)
#         logger.info(f"validating bronze for pipeline: {bronze_result['pipeline_id']}")
        
        
#         validation_checks = {
#             'null_checks': 'passed',
#             'duplicate_checks': 'passed',
#             'schema_validations': 'passed',
#             'row_counts':'passed'
            
#         }
        
#         return {
#             'status' : 'success',
#             'layer' : 'bronze_validate',
#             'pipeline_id': bronze_result['pipeline_id'],
#             'timestamp': datetime.now().isoformat(),
#             'validation_checks': validation_checks
#         }
    
#     @task 
#     def transform_silver_layer(bronze_validation):
#         import logging
#         from operators.dbt_operator import DbtOperator
#         from airflow import settings 
        
#         logger = logging.getlogger(__name__)
        
#         if bronze_validation['status'] == 'success':
#             raise Exception(f"Bronze validation failed, cannot with transformation: {bronze_validation}")
        
            
#         logger.info(f"Transforming silver layer for pipeline:{bronze_validation['pipeline_id']}")
            
#         operator = DbtOperator(
#                 task_id = 'transforms_silver_data_internal',
#                 dbt_root_dir=DBT_ROOT_DIR,
#                 dbt_command='run --select tag:silver',
                
#             )
            
#         try:
#                 operator.excute(context={})
#                 return {
#                     'status': 'success',
#                     'layer': 'silver_transforms',
#                     'pipeline_id': bronze_validation['pipeline_id'],
#                     'timestamp': datetime.now().isoformat(),    
                    
#                 }
#         except Exception as e:
#                 logger.warning(f'Error occured during transformation: {e}') 
#                 raise  
     
            
#     @task
#     def validated_silver_data(silver_result):
#         import logging
#         logger = logging.getLogger(__name__)
#         logger.info(f"validating bronze for pipeline: {silver_result['pipeline_id']}")
        
        
#         validation_checks = {
#             'business_rules': 'passed',
#             'referential_integrity': 'passed',
#             'aggregation_accuracy': 'passed',
#             'data_freshness':'passed'
            
#         }
        
#         return {
#             'status' : 'success',
#             'layer' : 'silver_validate',
#             'pipeline_id': silver_result['pipeline_id'],
#             'timestamp': datetime.now().isoformat(),
#             'validation_checks': validation_checks
#         }        
        
    
#     @task 
#     def transform_gold_layer(silver_validation):
#         import logging
#         from operators.dbt_operator import DbtOperator
#         from airflow import settings 
        
#         logger = logging.getlogger(__name__)
        
#         if silver_validation['status'] == 'success':
#             raise Exception(f"Silver validation failed, cannot with transformation: {silver_validation}")
        
            
#         logger.info(f"Transforming gold layer for pipeline:{silver_validation['pipeline_id']}")
            
#         operator = DbtOperator(
#                 task_id = 'transforms_gold_data_internal',
#                 dbt_root_dir=DBT_ROOT_DIR,
#                 dbt_command='run --select tag:gold',
                
#             )
            
#         try:
#                 operator.excute(context={})
#                 return {
#                     'status': 'success',
#                     'layer': 'gold_transforms',
#                     'pipeline_id': silver_validation['pipeline_id'],
#                     'timestamp': datetime.now().isoformat(),    
                    
#                 }
#         except Exception as e:
#                 logger.warning(f'Error occured during transformation: {e}') 
#                 raise      
            
#     @task
#     def validated_gold_data(gold_result):
#         import logging
#         logger = logging.getLogger(__name__)
#         logger.info(f"validating gold for pipeline: {gold_result['pipeline_id']}")
        
        
#         validation_checks = {
#             'business_rules': 'passed',
#             'metrics_calculation': 'passed',
#             'completeness_checks': 'passed',
#             'kpi_accuracy':'passed'
            
#         }
        
#         return {
#             'status' : 'success',
#             'layer' : 'gold_validate',
#             'pipeline_id': gold_result['pipeline_id'],
#             'timestamp': datetime.now().isoformat(),
#             'validation_checks': validation_checks
#         }        
#     @task
#     def generate_documentation(gold_validation):
#         import logging
#         from operators.dbt_operator import DbtOperator
        
#         logger = logging.getLogger(__name__)
        
#         if gold_validation['status'] != 'success':
#             raise Exception(f'Gold validation failed, cannot proceed with documentation: {gold_validation}')
        
#         logger.info(f"Generating documentation for pipeline: {gold_validation['pipeline_id']}")
        
#         operator = DbtOperator (
#             task_id='generate_dbt_docs_internal',
#             dbt_root_dir=DBT_ROOT_DIR,
#             dbt_command='docs generate'
#         ) 
        
#         try:
#             operator.excute(context={})
#             return {
#                 'status' : 'success',
#                 'layer' : 'gold_validate',
#                 'pipeline_id': gold_validation['pipeline_id'],
#                 'timestamp': datetime.now().isoformat(),
#             }
#         except Exception as e:
#             logger.warning(f'Error occured during documentation generation: {e}')    
#             raise
        
#     @task
#     def end_pipeline(docs_result,gold_validation):
#             import logging
#             logger = logging.getLogger(__name__)
#             logger.info('Ending pipeline')
            
#             logger.info(f"Pipeline completed successfully for pipeline: {gold_validation['pipeline_id']}")
#             logger.info(f"Final status: {gold_validation}")
#             logger.info(f'Pipeline completed at: {datetime.now().isoformat()}')
            
#             if docs_result['status'] != 'success':
#                 logger.warning(f'Documentation generation had issues but pipeline completed successfully')
            
            
#             pipeline_metadata = start_pipeline()
#             seed_result = seed_bronze(pipeline_metadata)
#             bronze_result = transform_bronze_layer(seed_result)
#             bronze_validation = validated_bronze_data(bronze_result)
#             silver_result = transform_silver_layer(bronze_validation)
#             silver_validation = validated_silver_data(silver_result)
#             gold_result = transform_gold_layer(silver_validation)
#             gold_validation = validated_gold_data(gold_result)
#             docs_result = generate_documentation(gold_validation)
#             end_pipeline(docs_result, gold_validation)
            
# dag =  dag_pipeline()           



from datetime import timedelta, datetime
from airflow import settings
from airflow.decorators import dag, task

DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/ecommerce_dbt"

# FIX 1: Removed stray ) after catchup=False, moved tags/max_active_runs inside the decorator.
# FIX 2: strat_date -> start_date
@dag(
    dag_id="ecommerce_dag_pipeline",
    default_args={
        "owner": "data-engineering-team",
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=15)
    },
    schedule=timedelta(hours=6),
    start_date=datetime(2026, 5, 4),
    catchup=False,
    tags=['dbt', 'medallion', 'ecommerce', 'analytics'],
    max_active_runs=1
)
def dag_pipeline():

    @task
    def start_pipeline():
        import logging
        logger = logging.getLogger(__name__)

        # FIX 3: logging.info -> logger.info, "pipline" -> "pipeline"
        logger.info("Starting pipeline")
        print("Hello World!")

        # FIX 4: datetime.now(),isoformat() -> datetime.now().isoformat()
        # FIX 5: 'dbt)root_dir' -> 'dbt_root_dir'
        # FIX 6: Added missing comma after pipeline_id entry
        pipeline_metadata = {
            "pipeline_start_time": datetime.now().isoformat(),
            'dbt_root_dir': DBT_ROOT_DIR,
            'pipeline_id': f'dag_pipeline_{datetime.now().strftime("%Y%m%d%H%M%S")}',
            'environment': 'production'
        }

        logger.info(f'Starting pipeline with ID: {pipeline_metadata["pipeline_id"]}')

        return pipeline_metadata

    @task
    def seed_bronze(pipeline_metadata):
        import logging
        from operators.dbt_operator import DbtOperator

        logger = logging.getLogger(__name__)
        logger.info('Seeding bronze...')

        try:
            import sqlalchemy
            from sqlalchemy import text

            engine = sqlalchemy.create_engine('trino://trino@trino-coordinator:8080/iceberg/bronze')
            with engine.connect() as conn:
                # FIX 7: conn.excute -> conn.execute
                result = conn.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                row_count = result.scalar()

                if row_count and row_count > 0:
                    logger.info(f"Bronze already seeded with {row_count} rows, skipping seeding")
                    return {
                        'status': 'skipped',
                        'layer': 'bronze_seed',
                        'pipeline_id': pipeline_metadata['pipeline_id'],
                        'timestamp': datetime.now().isoformat(),
                        'message': f'Tables already seeded with {row_count} rows'
                    }

        except Exception as e:
            logger.info(f"Tables do not exist or error occurred: {e}, proceeding with seeding")

            operator = DbtOperator(
                task_id='seed_bronze_data_internal',
                dbt_root_dir=DBT_ROOT_DIR,
                dbt_command='seed',
                full_refresh=True,
            )

            try:
                # FIX 7 (cont): operator.excute -> operator.execute
                operator.execute(context={})
                return {
                    'status': 'success',
                    'layer': 'bronze_seed',
                    'pipeline_id': pipeline_metadata['pipeline_id'],
                    'timestamp': datetime.now().isoformat(),
                }
            except Exception as e:
                logger.warning(f"Error occurred during seeding: {e}")

                # FIX 8: Return 'failed' status instead of 'success' so downstream tasks can detect the failure
                return {
                    'status': 'failed',
                    'layer': 'bronze_seed',
                    'pipeline_id': pipeline_metadata['pipeline_id'],
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e)
                }

    @task
    def transform_bronze_layer(seed_result):
        import logging
        from operators.dbt_operator import DbtOperator

        # FIX 9: logging.getlogger -> logging.getLogger (capital L)
        logger = logging.getLogger(__name__)

        # FIX 10: Was checking == 'failed', meaning transform only ran when seeding failed.
        # Now correctly proceeds when seeding succeeded or was skipped.
        if seed_result['status'] == 'failed':
            raise Exception(f"Seeding failed, cannot proceed with bronze transformation: {seed_result.get('error', 'Unknown error')}")

        logger.info("Transforming bronze...")

        # FIX 11: Added missing comma after task_id value
        operator = DbtOperator(
            task_id='transforms_bronze_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:bronze'
        )

        try:
            # FIX 7 (cont): operator.excute -> operator.execute
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'bronze_transform',
                'pipeline_id': seed_result['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'rows_processed': 'check logs for details'
            }
        except Exception as e:
            logger.warning(f'Error occurred during transformation: {e}')
            raise

    @task
    def validated_bronze_data(bronze_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating bronze for pipeline: {bronze_result['pipeline_id']}")

        validation_checks = {
            'null_checks': 'passed',
            'duplicate_checks': 'passed',
            'schema_validations': 'passed',
            'row_counts': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'bronze_validate',
            'pipeline_id': bronze_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validation_checks': validation_checks
        }

    @task
    def transform_silver_layer(bronze_validation):
        import logging
        from operators.dbt_operator import DbtOperator

        # FIX 9 (cont): logging.getlogger -> logging.getLogger
        logger = logging.getLogger(__name__)

        # FIX 12: Inverted condition — was raising on success, now correctly raises on failure
        if bronze_validation['status'] != 'success':
            raise Exception(f"Bronze validation failed, cannot proceed with transformation: {bronze_validation}")

        logger.info(f"Transforming silver layer for pipeline: {bronze_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transforms_silver_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:silver',
        )

        try:
            # FIX 7 (cont): operator.excute -> operator.execute
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'silver_transform',
                'pipeline_id': bronze_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f'Error occurred during transformation: {e}')
            raise

    @task
    def validated_silver_data(silver_result):
        import logging
        logger = logging.getLogger(__name__)

        # FIX 13: Log message said "validating bronze" — corrected to "silver"
        logger.info(f"Validating silver for pipeline: {silver_result['pipeline_id']}")

        validation_checks = {
            'business_rules': 'passed',
            'referential_integrity': 'passed',
            'aggregation_accuracy': 'passed',
            'data_freshness': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'silver_validate',
            'pipeline_id': silver_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validation_checks': validation_checks
        }

    @task
    def transform_gold_layer(silver_validation):
        import logging
        from operators.dbt_operator import DbtOperator

        # FIX 9 (cont): logging.getlogger -> logging.getLogger
        logger = logging.getLogger(__name__)

        # FIX 14: Same inverted condition bug as silver — fixed to raise on failure, not success
        if silver_validation['status'] != 'success':
            raise Exception(f"Silver validation failed, cannot proceed with transformation: {silver_validation}")

        logger.info(f"Transforming gold layer for pipeline: {silver_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transforms_gold_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:gold',
        )

        try:
            # FIX 7 (cont): operator.excute -> operator.execute
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'gold_transform',
                'pipeline_id': silver_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f'Error occurred during transformation: {e}')
            raise

    @task
    def validated_gold_data(gold_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating gold for pipeline: {gold_result['pipeline_id']}")

        validation_checks = {
            'business_rules': 'passed',
            'metrics_calculation': 'passed',
            'completeness_checks': 'passed',
            'kpi_accuracy': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'gold_validate',
            'pipeline_id': gold_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validation_checks': validation_checks
        }

    @task
    def generate_documentation(gold_validation):
        import logging
        from operators.dbt_operator import DbtOperator

        logger = logging.getLogger(__name__)

        if gold_validation['status'] != 'success':
            raise Exception(f'Gold validation failed, cannot proceed with documentation: {gold_validation}')

        logger.info(f"Generating documentation for pipeline: {gold_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='generate_dbt_docs_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='docs generate'
        )

        try:
            # FIX 7 (cont): operator.excute -> operator.execute
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'docs_generate',
                'pipeline_id': gold_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f'Error occurred during documentation generation: {e}')
            raise

    @task
    def end_pipeline(docs_result, gold_validation):
        import logging
        logger = logging.getLogger(__name__)

        logger.info(f"Pipeline completed for pipeline: {gold_validation['pipeline_id']}")
        logger.info(f"Final status: {gold_validation}")
        logger.info(f'Pipeline completed at: {datetime.now().isoformat()}')

        if docs_result['status'] != 'success':
            logger.warning('Documentation generation had issues but pipeline completed successfully')

    # FIX 15: Task wiring moved here to DAG scope (was incorrectly placed inside end_pipeline task body).
    # Airflow builds the dependency graph from this outer function at parse time.
    pipeline_metadata = start_pipeline()
    seed_result = seed_bronze(pipeline_metadata)
    bronze_result = transform_bronze_layer(seed_result)
    bronze_validation = validated_bronze_data(bronze_result)
    silver_result = transform_silver_layer(bronze_validation)
    silver_validation = validated_silver_data(silver_result)
    gold_result = transform_gold_layer(silver_validation)
    gold_validation = validated_gold_data(gold_result)
    docs_result = generate_documentation(gold_validation)
    end_pipeline(docs_result, gold_validation)


dag = dag_pipeline()