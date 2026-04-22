from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel


app = FastAPI(title="Uncle Joe's Coffee API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client()

PROJECT_ID = "mgmt545final"
DATASET_ID = "uncle_joes"

LOCATIONS_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.locations`"
MENU_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.menu`"


def format_time_value(value) -> Optional[str]:
    if value is None:
        return None
    try:
        num = int(value)
        return f"{num:04d}"
    except (ValueError, TypeError):
        return str(value)


def format_hours(row: dict) -> str:
    days = [
        ("Monday", "hours_monday_open", "hours_monday_close"),
        ("Tuesday", "hours_tuesday_open", "hours_tuesday_close"),
        ("Wednesday", "hours_wednesday_open", "hours_wednesday_close"),
        ("Thursday", "hours_thursday_open", "hours_thursday_close"),
        ("Friday", "hours_friday_open", "hours_friday_close"),
        ("Saturday", "hours_saturday_open", "hours_saturday_close"),
        ("Sunday", "hours_sunday_open", "hours_sunday_close"),
    ]

    parts = []
    for day_name, open_col, close_col in days:
        open_val = format_time_value(row.get(open_col))
        close_val = format_time_value(row.get(close_col))

        if open_val and close_val:
            parts.append(f"{day_name}: {open_val}-{close_val}")
        else:
            parts.append(f"{day_name}: Closed")

    return " | ".join(parts)


def build_full_address(row: dict) -> str:
    address_parts = []

    if row.get("address_one"):
        address_parts.append(str(row["address_one"]).strip())

    if row.get("address_two"):
        address_parts.append(str(row["address_two"]).strip())

    city_state_zip_parts = []

    if row.get("city"):
        city_state_zip_parts.append(str(row["city"]).strip())

    if row.get("state"):
        city_state_zip_parts.append(str(row["state"]).strip())

    city_state = ", ".join(city_state_zip_parts)

    if row.get("zip_code"):
        zip_code = str(row["zip_code"]).strip()
        if city_state:
            city_state = f"{city_state} {zip_code}"
        else:
            city_state = zip_code

    if city_state:
        address_parts.append(city_state)

    if address_parts:
        return ", ".join(address_parts)

    if row.get("location_map_address"):
        return str(row["location_map_address"]).strip()

    return "Unavailable"


def normalize_location_record(record: dict) -> dict:
    for field in ["zip_code", "phone_number", "fax_number", "email", "location_map_address", "near_by"]:
        if record.get(field) is not None:
            record[field] = str(record[field])

    record["address"] = build_full_address(record)
    record["hours"] = format_hours(record)
    return record


class Location(BaseModel):
    id: str
    city: Optional[str] = None
    state: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    open_for_business: Optional[bool] = None
    wifi: Optional[bool] = None
    drive_thru: Optional[bool] = None
    door_dash: Optional[bool] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None
    fax_number: Optional[str] = None
    location_map_address: Optional[str] = None
    zip_code: Optional[str] = None
    near_by: Optional[str] = None


class MenuItem(BaseModel):
    id: str
    name: Optional[str] = None
    category: Optional[str] = None
    size: Optional[str] = None
    calories: Optional[int] = None
    price: Optional[float] = None


@app.get("/")
def root():
    return {"message": "Uncle Joe's Coffee API is running"}


@app.get("/locations", response_model=List[Location])
def get_locations(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
):
    query = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE (@state IS NULL OR state = @state)
          AND (@city IS NULL OR city = @city)
        ORDER BY state, city
        LIMIT 100
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("state", "STRING", state),
            bigquery.ScalarQueryParameter("city", "STRING", city),
        ]
    )

    rows = client.query(query, job_config=job_config).result()

    results = []
    for row in rows:
        record = dict(row)
        results.append(normalize_location_record(record))

    return results


@app.get("/locations/{location_id}", response_model=Location)
def get_location(location_id: str):
    query = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE id = @id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", location_id)
        ]
    )

    rows = list(client.query(query, job_config=job_config).result())

    if not rows:
        raise HTTPException(status_code=404, detail="Location not found")

    record = dict(rows[0])
    return normalize_location_record(record)


@app.get("/menu", response_model=List[MenuItem])
def get_menu():
    query = f"""
        SELECT id, name, category, size, calories, price
        FROM {MENU_TABLE}
        ORDER BY category, name
    """

    rows = client.query(query).result()
    return [dict(row) for row in rows]


@app.get("/menu/{item_id}", response_model=MenuItem)
def get_menu_item(item_id: str):
    query = f"""
        SELECT id, name, category, size, calories, price
        FROM {MENU_TABLE}
        WHERE id = @id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", item_id)
        ]
    )

    rows = list(client.query(query, job_config=job_config).result())

    if not rows:
        raise HTTPException(status_code=404, detail="Menu item not found")

    return dict(rows[0])