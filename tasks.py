import glob
import json
import os
import platform

import pandas
from invoke import task
from invoke.util import cd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas


@task
def run_migrations(context, env):
    config = json.loads(open(f"conf/{env}/config.json").read())
    database_name = config["database"]
    export_cmd = f'export SNOWFLAKE_PRIVATE_KEY_PATH="{config["private_key_file_path"]}.rsa" &&'
    if platform.system() == "Windows":
        # Does not work
        #export_cmd = f'$Env:SNOWFLAKE_PRIVATE_KEY_PATH="{config["private_key_file_path"]}.rsa"| '
        export_cmd = ""
    context.run(f'{export_cmd} schemachange -f nyc_tlc/base_schema -a {config["account"]} -u {config["user"]} '
                f'-r {config["role"]} -w {config["warehouse"]} -d {database_name} '
                f'-c {database_name}.public.CHANGE_HISTORY --create-change-history-table')

@task
def run_dbt(context, env):
    with cd("nyc_tlc"):
        context.run(f"dbt --profiles-dir='../conf/{env}' run")


def sql_cmd(config, query):
    return f'snowsql -a {config["account"]} -u {config["user"]} -r {config["role"]} ' \
           f'--private-key-path {config["private_key_file_path"]}.rsa -q "{query}"'


def load_test_data(path, config):
    with connection(config) as conn:
        for filepath in glob.glob(path):
            table_name = os.path.splitext(os.path.split(filepath)[1])[0]
            print(f"Loading into {table_name}")
            write_pandas(conn=conn,
                         df=pandas.read_json(filepath),
                         table_name=table_name,
                         quote_identifiers=False
                         )


@task
def run_integration_tests(context, env="ci", parallelism=1):
    config = json.loads(open(f"conf/{env}/config.json").read())
    database_name = config["database"]
    try:
        context.run(sql_cmd(config, f'create database {database_name}'))
        run_migrations(context, "ci")
        load_test_data("tests/integration/data/*.json", config)
        create_dbt_profile("conf/ci", config)
        run_dbt(context, "ci")
        context.run(f"pytest -n {parallelism} tests/integration")
    finally:
        context.run(sql_cmd(config, f'drop database {database_name}'))



def create_dbt_profile(path, config):
    with open(f"{path}/profiles.yml", "w+") as f:
        f.write(f'''nyc_tlc:
  outputs:
    ci:
      account: {config["account"]}
      database: {config["database"]}
      warehouse: {config["warehouse"]}
      schema: {config["schema"]}
      private_key_path: ../{config["private_key_file_path"]}.rsa
      role: {config["role"]}
      threads: 1
      type: snowflake
      user: {config["user"]}
  target: ci
        ''')


def connection(config):
    return connect(
        private_key=(open(f'{config["private_key_file_path"]}.der', "rb").read()),
        user=config["user"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        role=config["role"],
        protocol='https')