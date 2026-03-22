from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
import io
import os
import requests

app = FastAPI()

# ================= ENV CONFIG =================

CY24_PATH = os.getenv("CY24_PATH", "data/CY24.csv")
CY25_PATH = os.getenv("CY25_PATH", "data/CY25.csv")
CY26_PATH = os.getenv("CY26_PATH", "data/CY26.csv")

GSHEET_CSV = os.getenv(
    "GSHEET_CSV",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSqiJ-d8D6IFLqWoBSwYyDG5-gewEzAob_CvM6CGC-Y8u_VAe_u8YklXn5nzR3DwtJBMNaxJQCf_Zmr/pub?output=csv"
)

# ================= GLOBAL =================

df = pd.DataFrame()
df_cancelled = pd.DataFrame()
ORIGINAL_CSV_COLS = []

# ================= HELPERS =================

def indian_format(n):
    n = round(float(n))
    return f"{n:,}"

def load_data():
    global ORIGINAL_CSV_COLS

    df1 = pd.read_csv(CY24_PATH, low_memory=False)
    df2 = pd.read_csv(CY25_PATH, low_memory=False)
    df3 = pd.read_csv(CY26_PATH, low_memory=False)

    df1["CY"] = "CY24"
    df2["CY"] = "CY25"
    df3["CY"] = "CY26"

    df = pd.concat([df1, df2, df3], ignore_index=True)
    df.columns = df.columns.str.strip()

    ORIGINAL_CSV_COLS = list(df.columns)

    # ===== ENRICH =====
    df["Division"] = df["Repair Order#"].astype(str).str[2:6]
    df["SA Name"] = df["RO Owner First Name"].astype(str) + " " + df["RO Owner Last Name"].astype(str)
    df["Model"] = df["Vehicle Model"].astype(str).str.split().str[0]

    # ===== DATE =====
    for col in ["Invoice Date", "Repair Order Date"]:
        df[col] = pd.to_datetime(df[col].astype(str).str[:10], errors="coerce", dayfirst=True)

    df["Month"] = df["Invoice Date"].dt.strftime("%b")
    df["TAT (Days)"] = (df["Invoice Date"] - df["Repair Order Date"]).dt.days

    # ===== AMOUNT =====
    for col in ["Net Taxable Labor Amount", "Net Taxable Parts Amt"]:
        df[col] = (
            df[col].astype(str)
            .str.replace("Rs.", "", regex=False)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
        )

    # ===== CANCELLED =====
    status_col = None
    for c in df.columns:
        if c.lower() in ["status", "invoice status", "ro status"]:
            status_col = c
            break

    if status_col:
        cancelled_mask = df[status_col].astype(str).str.lower() == "cancelled"
    else:
        cancelled_mask = pd.Series(False, index=df.index)

    df_cancelled = df[cancelled_mask].copy()
    df_active = df[~cancelled_mask].copy()

    return df_active, df_cancelled


@app.on_event("startup")
def startup():
    global df, df_cancelled
    df, df_cancelled = load_data()


# ================= GOOGLE SHEET =================

def load_gsheet():
    try:
        resp = requests.get(GSHEET_CSV, timeout=5)
        resp.raise_for_status()

        from io import StringIO
        gdf = pd.read_csv(StringIO(resp.text), low_memory=False)
        gdf.columns = gdf.columns.str.strip()

        # SAME LOGIC APPLY
        for col in ["Invoice Date", "Repair Order Date"]:
            if col in gdf.columns:
                gdf[col] = pd.to_datetime(gdf[col].astype(str).str[:10], errors="coerce", dayfirst=True)

        gdf["Month"] = gdf["Invoice Date"].dt.strftime("%b")
        gdf["CY"] = "CY26"

        return gdf

    except Exception as e:
        return pd.DataFrame()


# ================= FILTER =================

def apply_filters(data, division, service, sa, invoice, month, model, cy):
    if cy: data = data[data["CY"] == cy]
    if division: data = data[data["Division"].isin(division)]
    if service: data = data[data["Service Type"].isin(service)]
    if sa: data = data[data["SA Name"].isin(sa)]
    if invoice: data = data[data["Invoice Type"].isin(invoice)]
    if month: data = data[data["Month"].isin(month)]
    if model: data = data[data["Model"].isin(model)]
    return data


# ================= FILTER API =================

@app.get("/filters")
def filters():
    return {
        "division": sorted(df["Division"].dropna().unique().tolist()),
        "service": sorted(df["Service Type"].dropna().unique().tolist()),
        "sa": sorted(df["SA Name"].dropna().unique().tolist()),
        "invoice": sorted(df["Invoice Type"].dropna().unique().tolist()),
        "model": sorted(df["Model"].dropna().unique().tolist()),
        "month": sorted(df["Month"].dropna().unique().tolist()),
    }


# ================= CARDS =================

@app.get("/cards")
def cards(
    division: list[str] = Query(None),
    service: list[str] = Query(None),
    sa: list[str] = Query(None),
    invoice: list[str] = Query(None),
    month: list[str] = Query(None),
    model: list[str] = Query(None),
    cy: str = None,
):
    data = apply_filters(df.copy(), division, service, sa, invoice, month, model, cy)

    labour = data["Net Taxable Labor Amount"].sum()
    spares = data["Net Taxable Parts Amt"].sum()

    return {
        "inflow": int(data["Repair Order#"].nunique()),
        "labour": "₹ " + indian_format(labour),
        "spares": "₹ " + indian_format(spares),
        "total": "₹ " + indian_format(labour + spares)
    }


# ================= TABLE =================

@app.get("/table")
def table(
    division: list[str] = Query(None),
    service: list[str] = Query(None),
    sa: list[str] = Query(None),
    invoice: list[str] = Query(None),
    month: list[str] = Query(None),
    model: list[str] = Query(None),
    cy: str = None,
):
    data = apply_filters(df.copy(), division, service, sa, invoice, month, model, cy)

    data["Total"] = data["Net Taxable Labor Amount"] + data["Net Taxable Parts Amt"]

    return data.head(500).to_dict(orient="records")


# ================= CURRENT MONTH =================

@app.get("/current-month")
def current_month():
    gdf = load_gsheet()

    if gdf.empty:
        return {"error": "Google Sheet not available"}

    labour = gdf["Net Taxable Labor Amount"].sum()
    spares = gdf["Net Taxable Parts Amt"].sum()

    return {
        "inflow": int(gdf["Repair Order#"].nunique()),
        "labour": indian_format(labour),
        "spares": indian_format(spares),
        "total": indian_format(labour + spares)
    }


# ================= EXPORT =================

@app.get("/export")
def export():
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Invoice Data")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Renault_Export.xlsx"}
    )
