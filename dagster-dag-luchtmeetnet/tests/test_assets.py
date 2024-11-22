from typing import Dict, Union

import pytest
from dagster import materialize
from pytest_httpx import HTTPXMock

from luchtmeetnet.assets import (
    air_quality_data,
    daily_air_quality_data,
    daily_avg_air_quality_data,
)


@pytest.fixture(scope="module")
def mock_luchtmeetnet_api_response_data() -> list[dict[str, Union[str, float]]]:
    return [
        {
            "station_number": "NL01493",
            "value": 10.5,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "NO",
        },
        {
            "station_number": "NL01493",
            "value": 26.7,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "NO2",
        },
        {
            "station_number": "NL01493",
            "value": 3.4,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "O3",
        },
        {
            "station_number": "NL01493",
            "value": 11.5,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "PM25",
        },
        {
            "station_number": "NL01493",
            "value": 17.4,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "PM10",
        },
        {
            "station_number": "NL01493",
            "value": 0.93,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "FN",
        },
        {
            "station_number": "NL01493",
            "value": 0.17,
            "timestamp_measured": "2024-11-07T23:00:00+00:00",
            "formula": "BCWB",
        },
        {
            "station_number": "NL01493",
            "value": 17.6,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "NO",
        },
        {
            "station_number": "NL01493",
            "value": 30.1,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "NO2",
        },
        {
            "station_number": "NL01493",
            "value": -0.9,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "O3",
        },
        {
            "station_number": "NL01493",
            "value": 11.2,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "PM25",
        },
        {
            "station_number": "NL01493",
            "value": 16.6,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "PM10",
        },
        {
            "station_number": "NL01493",
            "value": 1.05,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "FN",
        },
        {
            "station_number": "NL01493",
            "value": 0.24,
            "timestamp_measured": "2024-11-07T22:00:00+00:00",
            "formula": "BCWB",
        },
        {
            "station_number": "NL01493",
            "value": 20.4,
            "timestamp_measured": "2024-11-07T21:00:00+00:00",
            "formula": "NO",
        },
    ]


def test_dag(
    resources: Dict[str, object],
    mock_luchtmeetnet_api_response_data: dict,
    httpx_mock: HTTPXMock,
):
    # NB: mock calls to the LMN API because it has limited historical data available
    httpx_mock.add_response(
        status_code=200,
        url="https://api.luchtmeetnet.nl/open_api/measurements?start=2024-11-07T00%3A00%3A00&end=2024-11-07T23%3A59%3A59&station_number=NL01493&page=1",
        json={"data": mock_luchtmeetnet_api_response_data},
    )

    res = materialize(
        assets=[air_quality_data],
        resources=resources,
        partition_key="2024-11-07|NL01493",
        selection=[air_quality_data],
    )
    res = materialize(
        assets=[air_quality_data, daily_air_quality_data],
        resources=resources,
        partition_key="2024-11-07",
        selection=[daily_air_quality_data],
    )
    res = materialize(
        assets=[air_quality_data, daily_air_quality_data, daily_avg_air_quality_data],
        resources=resources,
        selection=[daily_avg_air_quality_data],
    )
    assert res.success
