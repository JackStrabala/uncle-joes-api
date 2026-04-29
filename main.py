from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Response, Cookie
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
import bcrypt

app = FastAPI(title="Uncle Joe's Coffee API")

# CORS — frontend uses sessionStorage for auth, so credentials are not needed.
# Wildcard origin is fine here because we don't send cookies cross-origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client()

PROJECT_ID = "mgmt545final"
DATASET_ID = "uncle_joes"

LOCATIONS_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.locations`"
MENU_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.menu`"

MEMBERS_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.members`"
ORDERS_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.orders`"
ORDER_ITEMS_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.order_items`"

# =====================================================================
# Pydantic Models
# =====================================================================

class LoginRequest(BaseModel):
    email: str
    password: str

class LoginResponse(BaseModel):
    success: bool
    message: str
    member_id: Optional[str] = None

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

class OrderItem(BaseModel):
    item_name: Optional[str] = None
    size: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None

class MemberOrder(BaseModel):
    order_id: str
    order_date: Optional[str] = None
    store_location: Optional[str] = None
    order_total: Optional[float] = None
    items: List[OrderItem] = []

class PointsBalance(BaseModel):
    member_id: str
    points_balance: int

class HomeStore(BaseModel):
    id: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    address: Optional[str] = None

class MemberProfile(BaseModel):
    member_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None
    home_store: Optional[HomeStore] = None

class MemberStats(BaseModel):
    member_id: str
    total_orders: int
    total_spent: float
    average_order: float
    favorite_item: Optional[str] = None
    favorite_item_quantity: Optional[int] = None
    favorite_store: Optional[str] = None
    favorite_store_visits: Optional[int] = None
    first_order_date: Optional[str] = None
    last_order_date: Optional[str] = None

# =====================================================================
# Helper Functions
# =====================================================================

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
        city_state = f"{city_state} {zip_code}" if city_state else zip_code

    if city_state:
        address_parts.append(city_state)

    if address_parts:
        return ", ".join(address_parts)
    return str(row.get("location_map_address", "Unavailable")).strip()

def normalize_location_record(record: dict) -> dict:
    for field in ["zip_code", "phone_number", "fax_number", "email", "location_map_address", "near_by"]:
        if record.get(field) is not None:
            record[field] = str(record[field])
    record["address"] = build_full_address(record)
    record["hours"] = format_hours(record)
    return record

# =====================================================================
# Root
# =====================================================================

@app.get("/")
def root():
    return {"message": "Uncle Joe's Coffee API is running"}

# =====================================================================
# Auth
# =====================================================================

@app.post("/login", response_model=LoginResponse)
def login(login_data: LoginRequest, response: Response):
    query = f"""
        SELECT id, email, password
        FROM {MEMBERS_TABLE}
        WHERE email = @email
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", login_data.email)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())

    if not results:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    member = results[0]
    db_password = member.get('password')

    if not db_password:
        raise HTTPException(status_code=401, detail="Account not set up")

    password_byte = login_data.password.encode('utf-8')
    hash_byte = db_password.encode('utf-8')

    try:
        if bcrypt.checkpw(password_byte, hash_byte):
            member_id_str = str(member.id)
            return {
                "success": True,
                "message": "Login successful",
                "member_id": member_id_str
            }
    except ValueError:
        raise HTTPException(
            status_code=500,
            detail="Server data error: Password in database is not properly hashed."
        )

    raise HTTPException(status_code=401, detail="Invalid email or password")

@app.post("/logout")
def logout(response: Response):
    # Frontend handles session cleanup via sessionStorage; this endpoint
    # is kept for symmetry / future cookie support.
    return {"success": True, "message": "Logged out successfully"}

@app.get("/auth/status")
def get_auth_status(session_user: Optional[str] = Cookie(None)):
    if session_user:
        return {"is_logged_in": True, "member_id": session_user}
    return {"is_logged_in": False}

# =====================================================================
# Locations
# =====================================================================

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
    return [normalize_location_record(dict(row)) for row in rows]

@app.get("/locations/{location_id}", response_model=Location)
def get_location(location_id: str):
    query = f"""
        SELECT * FROM {LOCATIONS_TABLE} WHERE id = @id LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", location_id)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise HTTPException(status_code=404, detail="Location not found")
    return normalize_location_record(dict(rows[0]))

# =====================================================================
# Menu
# =====================================================================

@app.get("/menu", response_model=List[MenuItem])
def get_menu(
    search: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
):
    query = f"""
        SELECT id, name, category, size, calories, price
        FROM {MENU_TABLE}
        WHERE (@category IS NULL OR category = @category)
          AND (@search IS NULL OR LOWER(name) LIKE LOWER(CONCAT('%', @search, '%')))
        ORDER BY category, name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("search", "STRING", search),
            bigquery.ScalarQueryParameter("category", "STRING", category),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row) for row in rows]

# IMPORTANT: /menu/categories must be declared before /menu/{item_id}
# so FastAPI does not match "categories" as an item_id.
@app.get("/menu/categories", response_model=List[str])
def get_menu_categories():
    query = f"""
        SELECT DISTINCT category
        FROM {MENU_TABLE}
        WHERE category IS NOT NULL
        ORDER BY category
    """
    rows = client.query(query).result()
    return [row["category"] for row in rows]

@app.get("/menu/{item_id}", response_model=MenuItem)
def get_menu_item(item_id: str):
    query = f"SELECT id, name, category, size, calories, price FROM {MENU_TABLE} WHERE id = @id LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", item_id)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise HTTPException(status_code=404, detail="Menu item not found")

    return dict(rows[0])

# =====================================================================
# Members
# =====================================================================

@app.get("/members/{member_id}/orders", response_model=List[MemberOrder])
def get_member_orders(member_id: str):
    query = f"""
        SELECT
            o.order_id,
            CAST(o.order_date AS STRING) AS order_date,
            o.order_total,
            l.city,
            l.state,
            oi.item_name,
            oi.size,
            oi.quantity,
            oi.price
        FROM {ORDERS_TABLE} o
        LEFT JOIN {LOCATIONS_TABLE} l
            ON o.store_id = l.id
        LEFT JOIN {ORDER_ITEMS_TABLE} oi
            ON o.order_id = oi.order_id
        WHERE o.member_id = @member_id
        ORDER BY o.order_date DESC, o.order_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("member_id", "STRING", member_id)
        ]
    )

    rows = client.query(query, job_config=job_config).result()

    orders = {}

    for row in rows:
        record = dict(row)
        order_id = str(record["order_id"])

        city = record.get("city")
        state = record.get("state")

        if city and state:
            store_location = f"{city}, {state}"
        elif city:
            store_location = city
        elif state:
            store_location = state
        else:
            store_location = "Unknown location"

        if order_id not in orders:
            orders[order_id] = {
                "order_id": order_id,
                "order_date": record.get("order_date"),
                "store_location": store_location,
                "order_total": float(record["order_total"]) if record.get("order_total") is not None else None,
                "items": [],
            }

        if record.get("item_name") is not None:
            orders[order_id]["items"].append(
                {
                    "item_name": record.get("item_name"),
                    "size": record.get("size"),
                    "quantity": int(record["quantity"]) if record.get("quantity") is not None else None,
                    "price": float(record["price"]) if record.get("price") is not None else None,
                }
            )

    return list(orders.values())

@app.get("/members/{member_id}/points", response_model=PointsBalance)
def get_member_points(member_id: str):
    query = f"""
        SELECT
            @member_id AS member_id,
            COALESCE(SUM(FLOOR(order_total)), 0) AS points_balance
        FROM {ORDERS_TABLE}
        WHERE member_id = @member_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("member_id", "STRING", member_id)
        ]
    )

    rows = list(client.query(query, job_config=job_config).result())
    record = dict(rows[0])

    return {
        "member_id": member_id,
        "points_balance": int(record["points_balance"]),
    }

@app.get("/members/{member_id}/stats", response_model=MemberStats)
def get_member_stats(member_id: str):
    summary_query = f"""
        SELECT
            COUNT(*) AS total_orders,
            COALESCE(SUM(order_total), 0) AS total_spent,
            COALESCE(AVG(order_total), 0) AS average_order,
            CAST(MIN(order_date) AS STRING) AS first_order_date,
            CAST(MAX(order_date) AS STRING) AS last_order_date
        FROM {ORDERS_TABLE}
        WHERE member_id = @member_id
    """

    favorite_item_query = f"""
        SELECT
            oi.item_name,
            SUM(oi.quantity) AS total_quantity
        FROM {ORDER_ITEMS_TABLE} oi
        JOIN {ORDERS_TABLE} o ON oi.order_id = o.order_id
        WHERE o.member_id = @member_id
          AND oi.item_name IS NOT NULL
        GROUP BY oi.item_name
        ORDER BY total_quantity DESC
        LIMIT 1
    """

    favorite_store_query = f"""
        SELECT
            l.city,
            l.state,
            COUNT(*) AS visits
        FROM {ORDERS_TABLE} o
        LEFT JOIN {LOCATIONS_TABLE} l ON o.store_id = l.id
        WHERE o.member_id = @member_id
        GROUP BY l.city, l.state
        ORDER BY visits DESC
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("member_id", "STRING", member_id)
        ]
    )

    summary_rows = list(client.query(summary_query, job_config=job_config).result())
    summary = dict(summary_rows[0])

    favorite_item_rows = list(client.query(favorite_item_query, job_config=job_config).result())
    favorite_item = dict(favorite_item_rows[0]) if favorite_item_rows else None

    favorite_store_rows = list(client.query(favorite_store_query, job_config=job_config).result())
    favorite_store = dict(favorite_store_rows[0]) if favorite_store_rows else None

    favorite_store_label = None
    if favorite_store:
        city = favorite_store.get("city")
        state = favorite_store.get("state")
        if city and state:
            favorite_store_label = f"{city}, {state}"
        elif city:
            favorite_store_label = city
        elif state:
            favorite_store_label = state

    return {
        "member_id": member_id,
        "total_orders": int(summary["total_orders"]),
        "total_spent": float(summary["total_spent"]),
        "average_order": float(summary["average_order"]),
        "favorite_item": favorite_item["item_name"] if favorite_item else None,
        "favorite_item_quantity": int(favorite_item["total_quantity"]) if favorite_item else None,
        "favorite_store": favorite_store_label,
        "favorite_store_visits": int(favorite_store["visits"]) if favorite_store else None,
        "first_order_date": summary.get("first_order_date"),
        "last_order_date": summary.get("last_order_date"),
    }

# Member profile MUST be declared last under /members/ — its catch-all path
# would otherwise swallow /orders, /points, and /stats.
@app.get("/members/{member_id}", response_model=MemberProfile)
def get_member_profile(member_id: str):
    query = f"""
        SELECT
            m.id AS member_id,
            m.first_name,
            m.last_name,
            m.email,
            m.phone_number,
            m.home_store AS home_store_id,
            l.city AS home_store_city,
            l.state AS home_store_state,
            l.address_one AS home_store_address_one,
            l.address_two AS home_store_address_two,
            l.zip_code AS home_store_zip
        FROM {MEMBERS_TABLE} m
        LEFT JOIN {LOCATIONS_TABLE} l
            ON m.home_store = l.id
        WHERE m.id = @member_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("member_id", "STRING", member_id)
        ]
    )

    rows = list(client.query(query, job_config=job_config).result())

    if not rows:
        raise HTTPException(status_code=404, detail="Member not found")

    record = dict(rows[0])

    home_store = None
    if record.get("home_store_id"):
        home_store_row = {
            "address_one": record.get("home_store_address_one"),
            "address_two": record.get("home_store_address_two"),
            "city": record.get("home_store_city"),
            "state": record.get("home_store_state"),
            "zip_code": record.get("home_store_zip"),
        }
        home_store = {
            "id": record.get("home_store_id"),
            "city": record.get("home_store_city"),
            "state": record.get("home_store_state"),
            "address": build_full_address(home_store_row),
        }

    return {
        "member_id": str(record["member_id"]),
        "first_name": record.get("first_name"),
        "last_name": record.get("last_name"),
        "email": record.get("email"),
        "phone_number": record.get("phone_number"),
        "home_store": home_store,
    }
