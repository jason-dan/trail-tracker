import datetime
import http
import json
import logging
from typing import Any

import boto3
import requests
from fastkml import kml

BASE_URL = "https://share.garmin.com/Feed/Share/jonahkohn"
S3_BUCKET = "trail-tracker-mapdata"
S3_KEY = "data.geojson"
GARMIN_TIME_FORMAT = "%Y-%m-%dT%H:%M:%Sz"

START_TIME = datetime.datetime(year=2024, month=7, day=5, tzinfo=datetime.timezone.utc)
LOGGER = logging.getLogger()


def lambda_handler(event, context):
    LOGGER.setLevel('INFO')
    s3 = boto3.client("s3")
    geojson = json.load(
        s3.get_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
        ).get("Body")
    )
    LOGGER.info("Fetched data from S3.")
    geojson_features: list[dict] = geojson["features"]
    points_feature: dict[str, Any] = geojson_features[0]

    latest_timestamp = START_TIME
    if len(geojson_features) > 1:
        latest_timestamp = datetime.datetime.fromisoformat(
            geojson_features[-1]["properties"]["ts_utc"]
        )
    LOGGER.info("Latest timestamp in S3 file is %s", latest_timestamp)

    get_params = (
        {
            "d1": (latest_timestamp + datetime.timedelta(seconds=1)).strftime(
                GARMIN_TIME_FORMAT
            )
        }
        if latest_timestamp
        else {}
    )
    response = requests.get(BASE_URL, get_params)
    if response.status_code != http.HTTPStatus.OK:
        LOGGER.error("Non-200 code from Garmin! %s", response)
        raise RuntimeError("Non-200 code from Garmin")

    k = kml.KML()
    k.from_string(response.text)

    kml_features = k._features[0]._features
    if not kml_features:
        LOGGER.info("No new data found. Exiting")
        return {}

    del geojson_features[-1]["properties"]["latest"]
    for new_kml_feature in kml_features[0]._features:
        if new_kml_feature.geometry.geom_type != "Point":
            continue

        timestamp: datetime.datetime = new_kml_feature.timeStamp
        coordinates = [
            round(new_kml_feature.geometry.x, 5),
            round(new_kml_feature.geometry.y, 5),
        ]
        points_coords: list[list[float]] = points_feature["geometry"]["coordinates"]
        points_coords.append(coordinates)
        geojson_features.append(
            {
                "type": "Feature",
                "properties": {"ts_utc": timestamp.isoformat()},
                "geometry": {"type": "Point", "coordinates": coordinates},
            }
        )

    geojson_features[-1]["properties"]["latest"] = True
    s3.put_object(
        Body=json.dumps(geojson, separators=(",", ":")).encode("utf-8"),
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        CacheControl="no-store",
    )
    LOGGER.info("Uploaded new version to S3.")
    return {}
