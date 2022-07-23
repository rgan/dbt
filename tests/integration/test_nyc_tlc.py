import json

import pandas
import pytest

from tasks import connection


@pytest.mark.parametrize('dbt_view_name,cols', [
    ("avg_trip_metrics_by_day", "average_cost, average_passenger_count, average_distance")
])
def test_nyc_tlc(dbt_view_name, cols):
    config = json.loads(open(f"conf/ci/config.json", encoding="utf-8").read())
    print(config)
    with connection(config) as conn:
        sql = f"select {cols} from {dbt_view_name}"
        actual = json.loads(pandas.read_sql(sql, conn).to_json(orient='records'))
        expected = json.loads(open(f"tests/integration/expected/{dbt_view_name}.json").read())
        assert len(actual) == len(expected)
        for item in expected:
            if item not in actual:
                assert False, f"item not found"
