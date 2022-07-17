from invoke import task
from invoke.util import cd


@task
def run_dbt(context, env):
    context.run("pip install -r dev-requirements.txt")
    with cd("nyc_tlc"):
        context.run(f"dbt --profiles-dir=../conf/{env} run")
