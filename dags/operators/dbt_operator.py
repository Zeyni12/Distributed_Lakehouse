# from airflow.sdk import BaseOperator
# from airflow.exceptions import AirflowException
# from dbt.cli.main import dbtRunner, dbtRunnerResult
# import os


# class DbtOperator(BaseOperator):
#     def __init__(
#         self,
#         dbt_root_dir:str,
#         dbt_command:str,
#         target: str = None,
#         select: str = None,
#         dbt_vars: dict = None,
#         full_refresh:bool = False,
#         **kwargs
#     ):
#         super().__init__(**kwargs)
#         self.dbt_root_dir = dbt_root_dir
#         self.dbt_command = dbt_command
#         self.target = target
#         self.select = select
#         self.dbt_vars = dbt_vars
#         self.full_refresh = full_refresh
#         self.runner = dbtRunner()
        
#     def excute(self, context: Context) -> Any:
#         if not os.path.exists(self.dbt_root_dir):
#             raise AirflowException(f*dbt_root_dir {self.dbt_root_dir} does not exist")
                                   
#         logs_dir = os.path.join(self.dbt_root_dir, "logs")
#         if not os.path.exists(logs_dir):
#             try:
#                os.makedirs(logs_dir, mode=0o7777)
#                self.log.info(f'Created logs directory {logs_dir}') 
#             except Exception as e:
#                 self.log.error(f"Failed to create logs directory {logs_dir}: {e}")
#                 raise AirflowException(f"Failed to create logs directory {logs_dir}: {e}")
#         # Ensure the directory is writable 
#         if not os.access(logs_dir, os.W_OK):
#             try:
#                 os.chmod(logs_dir, mode=0o7777)
#                 self.log.info(f"Set writable permissions for logs directory {logs_dir}")      
#             except Exception as e:    
#                self.log.error(f"Failed to set writable permissions for logs directory {logs_dir}: {e}")
#                raise AirflowException(f"Failed to set writable permissions for logs directory {logs_dir}: {e}") 
               
#         # Split the dbt_command if it contains multiple arguments
#         if isinstance(self.dbt_command, str):
#             command_parts = self.dbt_command.split()
#         else:
#             command_parts = [self.dbt_command]
            
#         command_args = command_parts + [
#             "--project-dir", self.dbt_root_dir,
#             "--profiles-dir", self.dbt_root_dir,
#         ]
        
#         if self.target:
#             command_args.extend(["--target", self.target])
        
#         if self.select:
#             command_args.extend(["--target", self.select]) 
            
#         if self.full_refresh:
#              command_args.extend(["--full-refresh"])    
            
#         if self.dbt_vars:
#             vars_string = ' '.join([f"{k}: {v}" for k, v in self.dbt_vars.items()])
#             command_args.extend(["--vars", vars_string])
            
#         self.log.info("Excuting dbt command: %s", *args.join(command_args))
        
#         res: dbtRunnerResult = self.runner.invoke(command_args)    
        
#         if res.success:
#             self.log.info('dbt command excuted successfully')
#             if res.result:
#                 try:
#                    for r in res.result:
#                        if hasatr(r,'error') and hasattr(r, 'status'):
#                            self.log.info(f"{r.node.name}: {r.status}")
#                 except TypeError:
#                    self.log.info(f"Command completed with result type: {type(res.result).__name__}")
#             else:
#                self.log.info('No results returned') 
               
#         else: 
#             self.log.error('dbt command faild!')
#             if res.exception:
#                  self.log.error(f"Exception: {res.exception}")
#             raise AirflowException(f"dbt command failed: {' '.join(command_args)}")                            
            
                        
               
                                            
import json
import os
from typing import Any

from airflow.exceptions import AirflowException
# FIX 1: Import Context alongside BaseOperator
from airflow.sdk import BaseOperator, Context
from dbt.cli.main import dbtRunner, dbtRunnerResult


class DbtOperator(BaseOperator):
    def __init__(
        self,
        dbt_root_dir: str,
        dbt_command: str,
        target: str = None,
        select: str = None,
        dbt_vars: dict = None,
        full_refresh: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dbt_root_dir = dbt_root_dir
        self.dbt_command = dbt_command
        self.target = target
        self.select = select
        self.dbt_vars = dbt_vars
        self.full_refresh = full_refresh
        self.runner = dbtRunner()

    # FIX 2: excute -> execute (method was unreachable from the DAG due to this typo)
    def execute(self, context: Context) -> Any:
        # FIX 3: f* -> f" (missing opening quote caused a SyntaxError on import)
        if not os.path.exists(self.dbt_root_dir):
            raise AirflowException(f"dbt_root_dir {self.dbt_root_dir} does not exist")

        logs_dir = os.path.join(self.dbt_root_dir, "logs")
        if not os.path.exists(logs_dir):
            try:
                # FIX 4: 0o7777 -> 0o755 (overly permissive; sticky+setuid bits are a security risk)
                os.makedirs(logs_dir, mode=0o755)
                self.log.info(f"Created logs directory {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to create logs directory {logs_dir}: {e}")
                raise AirflowException(f"Failed to create logs directory {logs_dir}: {e}")

        # Ensure the directory is writable
        if not os.access(logs_dir, os.W_OK):
            try:
                # FIX 4 (cont): 0o7777 -> 0o755
                os.chmod(logs_dir, 0o755)
                self.log.info(f"Set writable permissions for logs directory {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to set writable permissions for logs directory {logs_dir}: {e}")
                raise AirflowException(f"Failed to set writable permissions for logs directory {logs_dir}: {e}")

        # Split the dbt_command string into a list of argument parts
        if isinstance(self.dbt_command, str):
            command_parts = self.dbt_command.split()
        else:
            command_parts = [self.dbt_command]

        command_args = command_parts + [
            "--project-dir", self.dbt_root_dir,
            "--profiles-dir", self.dbt_root_dir,
        ]

        if self.target:
            command_args.extend(["--target", self.target])

        # FIX 5: "--target" -> "--select" (target was being passed twice, select was never sent to dbt)
        if self.select:
            command_args.extend(["--select", self.select])

        if self.full_refresh:
            command_args.extend(["--full-refresh"])

        if self.dbt_vars:
            # FIX 6: dbt expects a valid JSON/YAML string for --vars, not space-joined "k: v" pairs
            vars_string = json.dumps(self.dbt_vars)
            command_args.extend(["--vars", vars_string])

        # FIX 7: *args.join() -> " ".join() (*args was undefined; lists have no .join() method)
        self.log.info("Executing dbt command: %s", " ".join(command_args))

        res: dbtRunnerResult = self.runner.invoke(command_args)

        if res.success:
            self.log.info("dbt command executed successfully")
            if res.result:
                try:
                    for r in res.result:
                        # FIX 8: hasatr -> hasattr (typo caused NameError when inspecting results)
                        if hasattr(r, 'error') and hasattr(r, 'status'):
                            self.log.info(f"{r.node.name}: {r.status}")
                except TypeError:
                    self.log.info(f"Command completed with result type: {type(res.result).__name__}")
            else:
                self.log.info("No results returned")

        else:
            self.log.error("dbt command failed!")
            if res.exception:
                self.log.error(f"Exception: {res.exception}")
            raise AirflowException(f"dbt command failed: {' '.join(command_args)}")                                            
                                            