from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional
import pandas as pd
import io
import os
import requests
import logging

# ================= LOGGING =================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================= ENV CONFIG =================

CY24_PATH = os.getenv("CY24_PATH", "data/CY24.csv")
CY25_PATH = os.getenv("CY25_PATH", "data/CY25.csv")
CY26_PATH = os.getenv("CY26_PATH", "data/CY26.csv")

GSHEET_CSV = os.getenv(
    "GSHEET_CSV",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSqiJ-d8D6IFLqWoBSwYyDG5-gewEzAob_CvM6CGC-Y8u_VAe_u8YklXn5nzR3DwtJBMNaxJQCf_Zmr/pub?output=csv"
)

# ================= GLOBAL STATE =================

df = pd.DataFrame()
df_cancelled = pd.DataFrame()
ORIGINAL_CSV_COLS: list = []

# ================= HELPERS =================

def indian_format(n):
    try:
        n = round(float(n))
        return f"{n:,}"
    except Exception:
        return "0"


def load_data():
    """Load and merge CY24, CY25, CY26 CSVs. Returns (df_active, df_cancelled)."""
    global ORIGINAL_CSV_COLS

    frames = []
    for path, label in [(CY24_PATH, "CY24"), (CY25_PATH, "CY25"), (CY26_PATH, "CY26")]:
        if not os.path.exists(path):
            logger.warning(f"File not found, skipping: {path}")
            continue
        try:
            tmp = pd.read_csv(path, low_memory=False)
            tmp["CY"] = label
            frames.append(tmp)
            logger.info(f"Loaded {path}: {len(tmp)} rows")
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")

    if not frames:
        logger.error("No CSV files loaded. Returning empty DataFrames.")
        return pd.DataFrame(), pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined.columns = combined.columns.str.strip()
    ORIGINAL_CSV_COLS = list(combined.columns)

    # ===== ENRICH =====
    combined["Division"] = combined["Repair Order#"].astype(str).str[2:6]

    first = combined["RO Owner First Name"].astype(str).str.strip()
    last  = combined["RO Owner Last Name"].astype(str).str.strip()
    combined["SA Name"] = (first + " " + last).str.strip()

    combined["Model"] = combined["Vehicle Model"].astype(str).str.split().str[0]

    # ===== DATE PARSING =====
    for col in ["Invoice Date", "Repair Order Date"]:
        if col in combined.columns:
            combined[col] = pd.to_datetime(
                combined[col].astype(str).str[:10],
                errors="coerce",
                dayfirst=True
            )

    combined["Month"] = combined["Invoice Date"].dt.strftime("%b")
    combined["Year"]  = combined["Invoice Date"].dt.year.astype("Int64")

    combined["TAT (Days)"] = (
        combined["Invoice Date"] - combined["Repair Order Date"]
    ).dt.days

    # ===== AMOUNT CLEANING =====
    for col in ["Net Taxable Labor Amount", "Net Taxable Parts Amt"]:
        if col in combined.columns:
            combined[col] = (
                combined[col]
                .astype(str)
                .str.replace("Rs.", "", regex=False)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
                .fillna(0)
            )
        else:
            combined[col] = 0.0
            logger.warning(f"Column '{col}' not found — defaulting to 0.")

    # ===== CANCELLED SPLIT =====
    status_col = None
    for c in combined.columns:
        if c.lower().strip() in ["status", "invoice status", "ro status"]:
            status_col = c
            break

    if status_col:
        cancelled_mask = combined[status_col].astype(str).str.lower().str.strip() == "cancelled"
        logger.info(f"Status column: '{status_col}' | Cancelled rows: {cancelled_mask.sum()}")
    else:
        cancelled_mask = pd.Series(False, index=combined.index)
        logger.warning("No status column found. No rows marked as cancelled.")

    df_cancelled = combined[cancelled_mask].copy()
    df_active    = combined[~cancelled_mask].copy()

    logger.info(f"Active rows: {len(df_active)} | Cancelled rows: {len(df_cancelled)}")
    return df_active, df_cancelled


# ================= LIFESPAN (replaces deprecated on_event) =================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global df, df_cancelled
    logger.info("Starting up — loading data...")
    df, df_cancelled = load_data()
    logger.info("Data loaded successfully.")
    yield
    logger.info("Shutting down.")


# ================= APP =================

app = FastAPI(
    title="Reno Revenue API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================= GOOGLE SHEET =================

def load_gsheet() -> pd.DataFrame:
    """Fetch live data from published Google Sheet CSV."""
    try:
        resp = requests.get(GSHEET_CSV, timeout=10)
        resp.raise_for_status()

        from io import StringIO
        gdf = pd.read_csv(StringIO(resp.text), low_memory=False)
        gdf.columns = gdf.columns.str.strip()

        for col in ["Invoice Date", "Repair Order Date"]:
            if col in gdf.columns:
                gdf[col] = pd.to_datetime(
                    gdf[col].astype(str).str[:10],
                    errors="coerce",
                    dayfirst=True
                )

        if "Invoice Date" in gdf.columns:
            gdf["Month"] = gdf["Invoice Date"].dt.strftime("%b")

        gdf["CY"] = "CY26"

        for col in ["Net Taxable Labor Amount", "Net Taxable Parts Amt"]:
            if col in gdf.columns:
                gdf[col] = (
                    gdf[col]
                    .astype(str)
                    .str.replace("Rs.", "", regex=False)
                    .str.replace(",", "", regex=False)
                    .pipe(pd.to_numeric, errors="coerce")
                    .fillna(0)
                )
            else:
                gdf[col] = 0.0

        logger.info(f"Google Sheet loaded: {len(gdf)} rows")
        return gdf

    except Exception as e:
        logger.error(f"Google Sheet load failed: {e}")
        return pd.DataFrame()


# ================= FILTER HELPER =================

def apply_filters(
    data: pd.DataFrame,
    division: Optional[List[str]],
    service: Optional[List[str]],
    sa: Optional[List[str]],
    invoice: Optional[List[str]],
    month: Optional[List[str]],
    model: Optional[List[str]],
    cy: Optional[str],
) -> pd.DataFrame:
    if data.empty:
        return data
    if cy       and "CY"           in data.columns: data = data[data["CY"].isin([cy])]
    if division and "Division"     in data.columns: data = data[data["Division"].isin(division)]
    if service  and "Service Type" in data.columns: data = data[data["Service Type"].isin(service)]
    if sa       and "SA Name"      in data.columns: data = data[data["SA Name"].isin(sa)]
    if invoice  and "Invoice Type" in data.columns: data = data[data["Invoice Type"].isin(invoice)]
    if month    and "Month"        in data.columns: data = data[data["Month"].isin(month)]
    if model    and "Model"        in data.columns: data = data[data["Model"].isin(model)]
    return data


# ================= HEALTH CHECK =================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "active_rows": len(df),
        "cancelled_rows": len(df_cancelled),
    }


# ================= FILTERS API =================

@app.get("/filters")
def filters():
    if df.empty:
        return {"error": "No data loaded"}

    def safe_unique(col):
        if col in df.columns:
            return sorted(df[col].dropna().astype(str).unique().tolist())
        return []

    return {
        "division": safe_unique("Division"),
        "service":  safe_unique("Service Type"),
        "sa":       safe_unique("SA Name"),
        "invoice":  safe_unique("Invoice Type"),
        "model":    safe_unique("Model"),
        "month":    safe_unique("Month"),
        "cy":       safe_unique("CY"),
    }


# ================= CARDS API =================

@app.get("/cards")
def cards(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
):
    if df.empty:
        return {"error": "No data loaded"}

    data = apply_filters(df.copy(), division, service, sa, invoice, month, model, cy)

    labour = data["Net Taxable Labor Amount"].sum() if "Net Taxable Labor Amount" in data.columns else 0
    spares = data["Net Taxable Parts Amt"].sum()    if "Net Taxable Parts Amt"    in data.columns else 0
    inflow = data["Repair Order#"].nunique()        if "Repair Order#"            in data.columns else 0

    return {
        "inflow": int(inflow),
        "labour": "₹ " + indian_format(labour),
        "spares": "₹ " + indian_format(spares),
        "total":  "₹ " + indian_format(labour + spares),
    }


# ================= TABLE API =================

@app.get("/table")
def table(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
    limit:    int                 = Query(500, ge=1, le=5000),
):
    if df.empty:
        return []

    data = apply_filters(df.copy(), division, service, sa, invoice, month, model, cy)

    if "Net Taxable Labor Amount" in data.columns and "Net Taxable Parts Amt" in data.columns:
        data["Total"] = data["Net Taxable Labor Amount"] + data["Net Taxable Parts Amt"]

    # Convert date columns to string so JSON serialization doesn't fail
    for col in data.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        data[col] = data[col].astype(str)

    # Replace NaN/NaT with None for clean JSON
    data = data.where(pd.notnull(data), None)

    return data.head(limit).to_dict(orient="records")


# ================= CURRENT MONTH (Google Sheet) =================

@app.get("/current-month")
def current_month():
    gdf = load_gsheet()

    if gdf.empty:
        return JSONResponse(
            status_code=503,
            content={"error": "Google Sheet not available or returned no data."}
        )

    labour = gdf["Net Taxable Labor Amount"].sum() if "Net Taxable Labor Amount" in gdf.columns else 0
    spares = gdf["Net Taxable Parts Amt"].sum()    if "Net Taxable Parts Amt"    in gdf.columns else 0
    inflow = gdf["Repair Order#"].nunique()        if "Repair Order#"            in gdf.columns else 0

    return {
        "inflow": int(inflow),
        "labour": "₹ " + indian_format(labour),
        "spares": "₹ " + indian_format(spares),
        "total":  "₹ " + indian_format(labour + spares),
    }


# ================= CANCELLED CARDS =================

@app.get("/cancelled")
def cancelled(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
):
    if df_cancelled.empty:
        return {"count": 0, "message": "No cancelled records found."}

    data = apply_filters(df_cancelled.copy(), division, service, sa, invoice, month, model, cy)
    return {"count": int(len(data))}


# ================= EXPORT =================

@app.get("/export")
def export(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
):
    if df.empty:
        return JSONResponse(status_code=503, content={"error": "No data available to export."})

    data = apply_filters(df.copy(), division, service, sa, invoice, month, model, cy)

    # Stringify datetimes for Excel compatibility
    for col in data.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        data[col] = data[col].astype(str)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, sheet_name="Invoice Data")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Renault_Export.xlsx"}
    )
