from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
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
    if not os.path.exists(path):
        logger.warning(f"File not found, skipping: {path}")
        return pd.DataFrame()
    try:
        header = pd.read_csv(path, nrows=0, low_memory=False)
        header.columns = header.columns.str.strip()
        available = [c for c in KEEP_COLS if c in header.columns]
        tmp = pd.read_csv(path, usecols=available, low_memory=False, dtype=str)
        tmp.columns = tmp.columns.str.strip()
        tmp["CY"] = label
        logger.info(f"Loaded {path}: {len(tmp)} rows, {len(tmp.columns)} cols")
        return tmp
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return pd.DataFrame()


def enrich(combined: pd.DataFrame) -> pd.DataFrame:
    if "Repair Order#" in combined.columns:
        combined["Division"] = combined["Repair Order#"].str[2:6]

    first = combined.get("RO Owner First Name", pd.Series("", index=combined.index)).fillna("").str.strip()
    last  = combined.get("RO Owner Last Name",  pd.Series("", index=combined.index)).fillna("").str.strip()
    combined["SA Name"] = (first + " " + last).str.strip()

    if "Vehicle Model" in combined.columns:
        combined["Model"] = combined["Vehicle Model"].fillna("").str.split().str[0]

    for col in ["Invoice Date", "Repair Order Date"]:
        if col in combined.columns:
            combined[col] = pd.to_datetime(
                combined[col].str[:10], errors="coerce", dayfirst=True
            )

    if "Invoice Date" in combined.columns:
        combined["Month"]     = combined["Invoice Date"].dt.strftime("%b")
        combined["MonthNum"]  = combined["Invoice Date"].dt.month.astype("Int8")
        combined["Year"]      = combined["Invoice Date"].dt.year.astype("Int32")

    if "Invoice Date" in combined.columns and "Repair Order Date" in combined.columns:
        combined["TAT (Days)"] = (combined["Invoice Date"] - combined["Repair Order Date"]).dt.days.astype("Int16")

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

    for col in ["CY", "Division", "Service Type", "Invoice Type", "Model", "Month", "SA Name"]:
        if col in combined.columns:
            combined[col] = combined[col].astype("category")

    return combined


def load_data():
    frames = []
    for path, label in [(CY24_PATH, "CY24"), (CY25_PATH, "CY25"), (CY26_PATH, "CY26")]:
        tmp = read_csv_lean(path, label)
        if not tmp.empty:
            frames.append(tmp)
        gc.collect()

    if not frames:
        logger.error("No CSV files loaded.")
        return pd.DataFrame(), pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()

    combined = enrich(combined)
    gc.collect()

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
app = FastAPI(title="Reno Revenue API", version="2.0.0", lifespan=lifespan)

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

@app.get("/", include_in_schema=False)
def root():
    """Redirect root to the dashboard."""
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    """Serve the frontend dashboard HTML."""
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h2>Dashboard not found. Place dashboard.html next to main.py.</h2>", status_code=404)


@app.get("/health")
def health():
    mem_mb = df.memory_usage(deep=True).sum() / 1024**2 if not df.empty else 0
    return {
        "status": "ok",
        "active_rows": len(df),
        "cancelled_rows": len(df_cancelled),
        "df_mb": round(mem_mb, 1),
        "version": "2.0.0"
    }


@app.get("/filters")
def filters():
    if df.empty:
        return {"error": "No data loaded"}
    def safe_unique(col):
        if col in df.columns:
            vals = sorted(df[col].dropna().astype(str).unique().tolist())
            return [v for v in vals if v and v.strip() and v.lower() not in ["nan", "none", ""]]
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
    cancelled_count = int(df_cancelled.shape[0])
    return {
        "inflow":    inflow,
        "labour":    "₹ " + indian_format(labour),
        "spares":    "₹ " + indian_format(spares),
        "total":     "₹ " + indian_format(labour + spares),
        "cancelled": cancelled_count,
        "labour_raw": round(labour, 2),
        "spares_raw": round(spares, 2),
        "total_raw":  round(labour + spares, 2),
    }


@app.get("/monthly-trend")
def monthly_trend(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
):
    """Monthly revenue trend — labour + spares by month."""
    if df.empty:
        return []
    data = apply_filters(df, division, service, sa, invoice, None, model, cy)
    if "Month" not in data.columns or "MonthNum" not in data.columns:
        return []

    grp = (
        data.groupby(["MonthNum", "Month"], observed=True)
        .agg(
            labour=("Net Taxable Labor Amount", "sum"),
            spares=("Net Taxable Parts Amt", "sum"),
            inflow=("Repair Order#", "nunique"),
        )
        .reset_index()
        .sort_values("MonthNum")
    )
    grp["total"] = grp["labour"] + grp["spares"]
    result = []
    for _, row in grp.iterrows():
        result.append({
            "month":  str(row["Month"]),
            "labour": round(float(row["labour"]), 2),
            "spares": round(float(row["spares"]), 2),
            "total":  round(float(row["total"]), 2),
            "inflow": int(row["inflow"]),
        })
    return result


@app.get("/service-mix")
def service_mix(
    division: Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
):
    """Breakdown by Service Type — count and revenue."""
    if df.empty:
        return []
    data = apply_filters(df, division, None, sa, invoice, month, model, cy)
    if "Service Type" not in data.columns:
        return []
    grp = (
        data.groupby("Service Type", observed=True)
        .agg(
            count=("Repair Order#", "nunique"),
            labour=("Net Taxable Labor Amount", "sum"),
            spares=("Net Taxable Parts Amt", "sum"),
        )
        .reset_index()
    )
    grp["total"] = grp["labour"] + grp["spares"]
    grp = grp.sort_values("total", ascending=False)
    return [
        {
            "service": str(row["Service Type"]),
            "count":   int(row["count"]),
            "labour":  round(float(row["labour"]), 2),
            "spares":  round(float(row["spares"]), 2),
            "total":   round(float(row["total"]), 2),
        }
        for _, row in grp.iterrows()
    ]


@app.get("/sa-performance")
def sa_performance(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
    top:      int                 = Query(10, ge=1, le=50),
):
    """Top SA performance by total revenue."""
    if df.empty:
        return []
    data = apply_filters(df, division, service, None, invoice, month, model, cy)
    if "SA Name" not in data.columns:
        return []
    grp = (
        data.groupby("SA Name", observed=True)
        .agg(
            count=("Repair Order#", "nunique"),
            labour=("Net Taxable Labor Amount", "sum"),
            spares=("Net Taxable Parts Amt", "sum"),
        )
        .reset_index()
    )
    grp["total"] = grp["labour"] + grp["spares"]
    grp = grp[grp["SA Name"].astype(str).str.strip() != ""].sort_values("total", ascending=False).head(top)
    return [
        {
            "sa":     str(row["SA Name"]),
            "count":  int(row["count"]),
            "labour": round(float(row["labour"]), 2),
            "spares": round(float(row["spares"]), 2),
            "total":  round(float(row["total"]), 2),
        }
        for _, row in grp.iterrows()
    ]


@app.get("/model-mix")
def model_mix(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    cy:       Optional[str]       = Query(None),
):
    """Revenue breakdown by vehicle model."""
    if df.empty:
        return []
    data = apply_filters(df, division, service, sa, invoice, month, None, cy)
    if "Model" not in data.columns:
        return []
    grp = (
        data.groupby("Model", observed=True)
        .agg(
            count=("Repair Order#", "nunique"),
            labour=("Net Taxable Labor Amount", "sum"),
            spares=("Net Taxable Parts Amt", "sum"),
        )
        .reset_index()
    )
    grp["total"] = grp["labour"] + grp["spares"]
    grp = grp[grp["Model"].astype(str).str.strip() != ""].sort_values("total", ascending=False)
    return [
        {
            "model":  str(row["Model"]),
            "count":  int(row["count"]),
            "labour": round(float(row["labour"]), 2),
            "spares": round(float(row["spares"]), 2),
            "total":  round(float(row["total"]), 2),
        }
        for _, row in grp.iterrows()
    ]


@app.get("/cy-comparison")
def cy_comparison(
    division: Optional[List[str]] = Query(None),
    service:  Optional[List[str]] = Query(None),
    sa:       Optional[List[str]] = Query(None),
    invoice:  Optional[List[str]] = Query(None),
    month:    Optional[List[str]] = Query(None),
    model:    Optional[List[str]] = Query(None),
):
    """Year-over-year comparison across CY24, CY25, CY26."""
    if df.empty:
        return []
    data = apply_filters(df, division, service, sa, invoice, month, model, None)
    if "CY" not in data.columns:
        return []
    grp = (
        data.groupby("CY", observed=True)
        .agg(
            count=("Repair Order#", "nunique"),
            labour=("Net Taxable Labor Amount", "sum"),
            spares=("Net Taxable Parts Amt", "sum"),
        )
        .reset_index()
    )
    grp["total"] = grp["labour"] + grp["spares"]
    grp = grp.sort_values("CY")
    return [
        {
            "cy":     str(row["CY"]),
            "count":  int(row["count"]),
            "labour": round(float(row["labour"]), 2),
            "spares": round(float(row["spares"]), 2),
            "total":  round(float(row["total"]), 2),
        }
        for _, row in grp.iterrows()
    ]


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
        "inflow":     inflow,
        "labour":     "₹ " + indian_format(labour),
        "spares":     "₹ " + indian_format(spares),
        "total":      "₹ " + indian_format(labour + spares),
        "labour_raw": round(labour, 2),
        "spares_raw": round(spares, 2),
        "total_raw":  round(labour + spares, 2),
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
    labour = float(data["Net Taxable Labor Amount"].sum()) if "Net Taxable Labor Amount" in data.columns else 0
    spares = float(data["Net Taxable Parts Amt"].sum())    if "Net Taxable Parts Amt"    in data.columns else 0
    return {
        "count":  int(len(data)),
        "labour": "₹ " + indian_format(labour),
        "spares": "₹ " + indian_format(spares),
        "total":  "₹ " + indian_format(labour + spares),
    }


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

    # Add total column
    if "Net Taxable Labor Amount" in data.columns and "Net Taxable Parts Amt" in data.columns:
        data["Total Revenue"] = data["Net Taxable Labor Amount"] + data["Net Taxable Parts Amt"]

    # Stringify for Excel
    for col in data.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        data[col] = data[col].astype(str)
    for col in data.select_dtypes(include="category").columns:
        data[col] = data[col].astype(str)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, sheet_name="Invoice Data")

        # Summary sheet
        summary_data = {
            "Metric": ["Total ROs", "Labour Revenue", "Parts Revenue", "Total Revenue", "Cancelled ROs"],
            "Value": [
                data["Repair Order#"].nunique() if "Repair Order#" in data.columns else 0,
                round(float(data["Net Taxable Labor Amount"].sum()), 2) if "Net Taxable Labor Amount" in data.columns else 0,
                round(float(data["Net Taxable Parts Amt"].sum()), 2)    if "Net Taxable Parts Amt"    in data.columns else 0,
                round(float(data["Total Revenue"].sum()), 2)            if "Total Revenue"            in data.columns else 0,
                len(df_cancelled),
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, index=False, sheet_name="Summary")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Renault_Export.xlsx"}
    )
