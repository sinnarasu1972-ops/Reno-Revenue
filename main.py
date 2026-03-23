from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional
import pandas as pd
import numpy as np
import io
import os
import gc
import requests
import logging

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================= ENV CONFIG =================
CY24_PATH = os.getenv("CY24_PATH", "Invoice Report CY24.csv")
CY25_PATH = os.getenv("CY25_PATH", "Invoice Report CY25.csv")
CY26_PATH = os.getenv("CY26_PATH", "Invoice Report CY26.csv")

GSHEET_CSV = os.getenv(
    "GSHEET_CSV",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSqiJ-d8D6IFLqWoBSwYyDG5-gewEzAob_CvM6CGC-Y8u_VAe_u8YklXn5nzR3DwtJBMNaxJQCf_Zmr/pub?output=csv"
)

# ================= GLOBAL STATE =================
df = pd.DataFrame()
df_cancelled = pd.DataFrame()

# ================= COLUMNS TO KEEP =================
# Only load columns actually needed — drops everything else to save memory
KEEP_COLS = [
    "Repair Order#",
    "RO Owner First Name",
    "RO Owner Last Name",
    "Vehicle Model",
    "Invoice Date",
    "Repair Order Date",
    "Net Taxable Labor Amount",
    "Net Taxable Parts Amt",
    "Service Type",
    "Invoice Type",
    "Status",
    # alternate status column names
    "Invoice Status",
    "RO Status",
]

# ================= HELPERS =================
def indian_format(n):
    try:
        n = round(float(n))
        return f"{n:,}"
    except Exception:
        return "0"


def read_csv_lean(path: str, label: str) -> pd.DataFrame:
    """Read only needed columns, use smallest possible dtypes."""
    if not os.path.exists(path):
        logger.warning(f"File not found, skipping: {path}")
        return pd.DataFrame()

    try:
        # First pass: find which KEEP_COLS actually exist in this file
        header = pd.read_csv(path, nrows=0, low_memory=False)
        header.columns = header.columns.str.strip()
        available = [c for c in KEEP_COLS if c in header.columns]

        tmp = pd.read_csv(
            path,
            usecols=available,
            low_memory=False,
            dtype=str,           # read everything as str first — lowest memory during load
        )
        tmp.columns = tmp.columns.str.strip()
        tmp["CY"] = label
        logger.info(f"Loaded {path}: {len(tmp)} rows, {len(tmp.columns)} cols")
        return tmp
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return pd.DataFrame()


def enrich(combined: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns, parse dates, cast amounts."""

    # Division from RO number
    if "Repair Order#" in combined.columns:
        combined["Division"] = combined["Repair Order#"].str[2:6]

    # SA Name
    first = combined.get("RO Owner First Name", pd.Series("", index=combined.index)).fillna("").str.strip()
    last  = combined.get("RO Owner Last Name",  pd.Series("", index=combined.index)).fillna("").str.strip()
    combined["SA Name"] = (first + " " + last).str.strip()

    # Model — first word only
    if "Vehicle Model" in combined.columns:
        combined["Model"] = combined["Vehicle Model"].fillna("").str.split().str[0]

    # Dates
    for col in ["Invoice Date", "Repair Order Date"]:
        if col in combined.columns:
            combined[col] = pd.to_datetime(
                combined[col].str[:10], errors="coerce", dayfirst=True
            )

    if "Invoice Date" in combined.columns:
        combined["Month"] = combined["Invoice Date"].dt.strftime("%b")
        combined["Year"]  = combined["Invoice Date"].dt.year.astype("Int32")

    if "Invoice Date" in combined.columns and "Repair Order Date" in combined.columns:
        combined["TAT (Days)"] = (combined["Invoice Date"] - combined["Repair Order Date"]).dt.days.astype("Int16")

    # Amounts — convert to float32 (half the memory of float64)
    for col in ["Net Taxable Labor Amount", "Net Taxable Parts Amt"]:
        if col in combined.columns:
            combined[col] = (
                combined[col]
                .str.replace("Rs.", "", regex=False)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
                .fillna(0)
                .astype(np.float32)
            )
        else:
            combined[col] = np.float32(0)

    # Downcast string columns to category (major memory saving for low-cardinality cols)
    for col in ["CY", "Division", "Service Type", "Invoice Type", "Model", "Month", "SA Name"]:
        if col in combined.columns:
            combined[col] = combined[col].astype("category")

    return combined


def load_data():
    """Load CSVs one at a time, enrich, merge, split active/cancelled."""
    frames = []
    for path, label in [(CY24_PATH, "CY24"), (CY25_PATH, "CY25"), (CY26_PATH, "CY26")]:
        tmp = read_csv_lean(path, label)
        if not tmp.empty:
            frames.append(tmp)
        gc.collect()   # free memory after each file

    if not frames:
        logger.error("No CSV files loaded.")
        return pd.DataFrame(), pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()

    combined = enrich(combined)
    gc.collect()

    # Status split
    status_col = next(
        (c for c in combined.columns if c.lower().strip() in ["status", "invoice status", "ro status"]),
        None
    )

    if status_col:
        cancelled_mask = combined[status_col].astype(str).str.lower().str.strip() == "cancelled"
        logger.info(f"Status column: '{status_col}' | Cancelled: {cancelled_mask.sum()}")
    else:
        cancelled_mask = pd.Series(False, index=combined.index)
        logger.warning("No status column found.")

    df_cancelled = combined[cancelled_mask].copy()
    df_active    = combined[~cancelled_mask].copy()
    del combined
    gc.collect()

    logger.info(f"Active: {len(df_active)} rows | Cancelled: {len(df_cancelled)} rows")
    logger.info(f"Active DF memory: {df_active.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    return df_active, df_cancelled


# ================= LIFESPAN =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global df, df_cancelled
    logger.info("Starting up — loading data...")
    df, df_cancelled = load_data()
    logger.info("Startup complete.")
    yield
    logger.info("Shutting down.")


# ================= APP =================
app = FastAPI(title="Reno Revenue API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================= GOOGLE SHEET =================
def load_gsheet() -> pd.DataFrame:
    try:
        resp = requests.get(GSHEET_CSV, timeout=10)
        resp.raise_for_status()
        from io import StringIO
        gdf = pd.read_csv(StringIO(resp.text), low_memory=False, dtype=str)
        gdf.columns = gdf.columns.str.strip()
        for col in ["Invoice Date", "Repair Order Date"]:
            if col in gdf.columns:
                gdf[col] = pd.to_datetime(gdf[col].str[:10], errors="coerce", dayfirst=True)
        if "Invoice Date" in gdf.columns:
            gdf["Month"] = gdf["Invoice Date"].dt.strftime("%b")
        gdf["CY"] = "CY26"
        for col in ["Net Taxable Labor Amount", "Net Taxable Parts Amt"]:
            if col in gdf.columns:
                gdf[col] = (
                    gdf[col].str.replace("Rs.", "", regex=False)
                    .str.replace(",", "", regex=False)
                    .pipe(pd.to_numeric, errors="coerce").fillna(0)
                )
            else:
                gdf[col] = 0.0
        logger.info(f"GSheet loaded: {len(gdf)} rows")
        return gdf
    except Exception as e:
        logger.error(f"GSheet failed: {e}")
        return pd.DataFrame()


# ================= FILTER =================
def apply_filters(data, division, service, sa, invoice, month, model, cy):
    if data.empty:
        return data
    if cy       and "CY"           in data.columns: data = data[data["CY"].astype(str).isin([cy])]
    if division and "Division"     in data.columns: data = data[data["Division"].astype(str).isin(division)]
    if service  and "Service Type" in data.columns: data = data[data["Service Type"].astype(str).isin(service)]
    if sa       and "SA Name"      in data.columns: data = data[data["SA Name"].astype(str).isin(sa)]
    if invoice  and "Invoice Type" in data.columns: data = data[data["Invoice Type"].astype(str).isin(invoice)]
    if month    and "Month"        in data.columns: data = data[data["Month"].astype(str).isin(month)]
    if model    and "Model"        in data.columns: data = data[data["Model"].astype(str).isin(model)]
    return data


# ================= ENDPOINTS =================

@app.get("/health")
def health():
    mem_mb = df.memory_usage(deep=True).sum() / 1024**2 if not df.empty else 0
    return {"status": "ok", "active_rows": len(df), "cancelled_rows": len(df_cancelled), "df_mb": round(mem_mb, 1)}


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
    data = apply_filters(df, division, service, sa, invoice, month, model, cy)
    labour = float(data["Net Taxable Labor Amount"].sum()) if "Net Taxable Labor Amount" in data.columns else 0
    spares = float(data["Net Taxable Parts Amt"].sum())    if "Net Taxable Parts Amt"    in data.columns else 0
    inflow = int(data["Repair Order#"].nunique())          if "Repair Order#"            in data.columns else 0
    return {
        "inflow": inflow,
        "labour": "₹ " + indian_format(labour),
        "spares": "₹ " + indian_format(spares),
        "total":  "₹ " + indian_format(labour + spares),
    }


@app.get("/table")
def table(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
    limit:    int                 = Query(200, ge=1, le=1000),
):
    if df.empty:
        return []
    data = apply_filters(df, division, service, sa, invoice, month, model, cy)
    if "Net Taxable Labor Amount" in data.columns and "Net Taxable Parts Amt" in data.columns:
        data = data.copy()
        data["Total"] = data["Net Taxable Labor Amount"] + data["Net Taxable Parts Amt"]
    # Stringify dates and categories for JSON
    out = data.head(limit).copy()
    for col in out.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        out[col] = out[col].astype(str)
    for col in out.select_dtypes(include="category").columns:
        out[col] = out[col].astype(str)
    out = out.where(pd.notnull(out), None)
    return out.to_dict(orient="records")


@app.get("/current-month")
def current_month():
    gdf = load_gsheet()
    if gdf.empty:
        return JSONResponse(status_code=503, content={"error": "Google Sheet not available."})
    labour = float(gdf["Net Taxable Labor Amount"].sum()) if "Net Taxable Labor Amount" in gdf.columns else 0
    spares = float(gdf["Net Taxable Parts Amt"].sum())    if "Net Taxable Parts Amt"    in gdf.columns else 0
    inflow = int(gdf["Repair Order#"].nunique())          if "Repair Order#"            in gdf.columns else 0
    return {
        "inflow": inflow,
        "labour": "₹ " + indian_format(labour),
        "spares": "₹ " + indian_format(spares),
        "total":  "₹ " + indian_format(labour + spares),
    }


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
    data = apply_filters(df_cancelled, division, service, sa, invoice, month, model, cy)
    return {"count": int(len(data))}


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
        return JSONResponse(status_code=503, content={"error": "No data available."})
    data = apply_filters(df, division, service, sa, invoice, month, model, cy).copy()
    for col in data.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        data[col] = data[col].astype(str)
    for col in data.select_dtypes(include="category").columns:
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
