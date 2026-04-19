from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel

app = FastAPI(title="Uncle Joe's Coffee API")

# ✅ CORS (required)
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


# -----------------------
# Models
# -----------------------
class Location(BaseModel):
    id: str
    city: Optional[str] = None
    state: Optional[str] = None
    open_for_business: Optional[bool] = None
    wifi: Optional[bool] = None
    drive_thru: Optional[bool] = None
    door_dash: Optional[bool] = None
    email: Optional[str] = None


class MenuItem(BaseModel):
    id: str
    name: Optional[str] = None
    category: Optional[str] = None
    size: Optional[str] = None
    calories: Optional[int] = None
    price: Optional[float] = None


# -----------------------
# Root
# -----------------------
@app.get("/")
def root():
    return {"message": "API is working"}


# -----------------------
# GET ALL LOCATIONS
# -----------------------
@app.get("/locations", response_model=List[Location])
def get_locations(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None)
):
    query = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE (@state IS NULL OR state = @state)
        AND (@city IS NULL OR city = @city)
        LIMIT 100
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("state", "STRING", state),
            bigquery.ScalarQueryParameter("city", "STRING", city),
        ]
    )

    rows = client.query(query, job_config=job_config).result()
    return [dict(row) for row in rows]


# -----------------------
# GET SINGLE LOCATION
# -----------------------
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

    return dict(rows[0])


# -----------------------
# GET ALL MENU
# -----------------------
@app.get("/menu", response_model=List[MenuItem])
def get_menu():
    query = f"""
        SELECT *
        FROM {MENU_TABLE}
        ORDER BY category, name
    """

    rows = client.query(query).result()
    return [dict(row) for row in rows]


# -----------------------
# GET SINGLE MENU ITEM
# -----------------------
@app.get("/menu/{item_id}", response_model=MenuItem)
def get_menu_item(item_id: str):
    query = f"""
        SELECT *
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
        raise HTTPException(status_code=404, detail="Item not found")

    return dict(rows[0])