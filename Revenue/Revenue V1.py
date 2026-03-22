from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import pandas as pd
import uvicorn
import threading
import webbrowser

app = FastAPI()

CY24_PATH = r"E:\Renault\Invoice Report CY24.csv"
CY25_PATH = r"E:\Renault\Invoice Report CY25.csv"


# ---------------- LOAD DATA ----------------

def load_data():

    df1 = pd.read_csv(CY24_PATH, low_memory=False)
    df2 = pd.read_csv(CY25_PATH, low_memory=False)

    df1["CY"] = "CY24"
    df2["CY"] = "CY25"

    df = pd.concat([df1, df2], ignore_index=True)

    df.columns = df.columns.str.strip()

    # Division
    df["Division"] = df["Repair Order#"].astype(str).str[2:6]

    # SA Name
    df["SA Name"] = df["RO Owner First Name"].astype(str) + " " + df["RO Owner Last Name"].astype(str)

    # Invoice Date
    df["Invoice Date"] = df["Invoice Date"].astype(str).str.split(" - ").str[0]
    df["Invoice Date"] = pd.to_datetime(df["Invoice Date"], format="%d/%m/%Y", errors="coerce")

    df["Month"] = df["Invoice Date"].dt.strftime("%b")

    # Clean revenue
    df["Net Taxable Labor Amount"] = (
        df["Net Taxable Labor Amount"]
        .astype(str)
        .str.replace("Rs.", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    df["Net Taxable Parts Amt"] = (
        df["Net Taxable Parts Amt"]
        .astype(str)
        .str.replace("Rs.", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    return df


df = load_data()


# ---------------- FILTERS ----------------

@app.get("/filters")
def filters():

    return {
        "division": sorted(df["Division"].dropna().unique().tolist()),
        "service": sorted(df["Service Type"].dropna().unique().tolist()),
        "sa": sorted(df["SA Name"].dropna().unique().tolist()),
        "invoice": sorted(df["Invoice Type"].dropna().unique().tolist()),
        "month": sorted(df["Month"].dropna().unique().tolist())
    }


# ---------------- CARDS ----------------

@app.get("/cards")
def cards(cy: str = None):

    data = df if not cy else df[df["CY"] == cy]

    return {
        "inflow24": df[df["CY"] == "CY24"]["Repair Order#"].nunique(),
        "inflow25": df[df["CY"] == "CY25"]["Repair Order#"].nunique(),
        "labour": f"₹ {data['Net Taxable Labor Amount'].sum():,.2f}",
        "spares": f"₹ {data['Net Taxable Parts Amt'].sum():,.2f}"
    }


# ---------------- TABLE ----------------

@app.get("/table")
def table(
        division: list[str] = Query(None),
        service: list[str] = Query(None),
        sa: list[str] = Query(None),
        invoice: list[str] = Query(None),
        month: list[str] = Query(None),
        cy: str = None
):

    data = df

    if cy:
        data = data[data["CY"] == cy]

    if division:
        data = data[data["Division"].isin(division)]

    if service:
        data = data[data["Service Type"].isin(service)]

    if sa:
        data = data[data["SA Name"].isin(sa)]

    if invoice:
        data = data[data["Invoice Type"].isin(invoice)]

    if month:
        data = data[data["Month"].isin(month)]

    data = data.head(500)

    return data.to_dict(orient="records")


# ---------------- DASHBOARD ----------------

@app.get("/", response_class=HTMLResponse)
def dashboard():

    return HTMLResponse("""

<html>

<head>

<title>Renault Dashboard</title>

<link href="https://cdn.jsdelivr.net/npm/select2@4.1.0/dist/css/select2.min.css" rel="stylesheet" />

<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>

<script src="https://cdn.jsdelivr.net/npm/select2@4.1.0/dist/js/select2.min.js"></script>

<style>

body{
font-family:Arial;
background:linear-gradient(135deg,#4f6bdc,#7b4fdc);
margin:0;
}

.container{
background:white;
margin:40px;
padding:30px;
border-radius:15px;
}

.cards{
display:flex;
gap:20px;
margin-bottom:20px;
}

.card{
flex:1;
background:#f4f6ff;
padding:20px;
border-radius:10px;
text-align:center;
}

.filters{
display:grid;
grid-template-columns:repeat(6,1fr);
gap:15px;
margin-bottom:20px;
}

button{
padding:12px;
background:#4f6bdc;
color:white;
border:none;
border-radius:6px;
cursor:pointer;
}

table{
width:100%;
border-collapse:collapse;
}

th{
background:#f4f6ff;
padding:10px;
}

td{
padding:8px;
border-bottom:1px solid #eee;
}

</style>


<script>

function getValues(id){

let val = $('#'+id).val()

if(!val) return ""

return val.join("&"+id+"=")

}


async function loadFilters(){

let res = await fetch('/filters')
let data = await res.json()

fill("division",data.division)
fill("service",data.service)
fill("sa",data.sa)
fill("invoice",data.invoice)
fill("month",data.month)

$('.multi').select2()

}

function fill(id,list){

let el = document.getElementById(id)

list.forEach(v=>{
let op=document.createElement("option")
op.value=v
op.text=v
el.appendChild(op)
})

}


async function loadCards(){

let cy=document.getElementById("cy").value

let res=await fetch('/cards?cy='+cy)

let data=await res.json()

document.getElementById("inflow24").innerText=data.inflow24
document.getElementById("inflow25").innerText=data.inflow25
document.getElementById("labour").innerText=data.labour
document.getElementById("spares").innerText=data.spares

}


async function loadTable(){

let cy=document.getElementById("cy").value

let url="/table?cy="+cy

url+="&division="+getValues("division")
url+="&service="+getValues("service")
url+="&sa="+getValues("sa")
url+="&invoice="+getValues("invoice")
url+="&month="+getValues("month")

let res=await fetch(url)

let data=await res.json()

let body=document.getElementById("tbody")

body.innerHTML=""

data.forEach(r=>{

let row=`
<tr>
<td>${r.Division}</td>
<td>${r["Repair Order#"]}</td>
<td>${r["SA Name"]}</td>
<td>${r["Vehicle Reg#"]}</td>
<td>${r["Service Type"]}</td>
<td>₹ ${Number(r["Net Taxable Labor Amount"]).toLocaleString()}</td>
<td>₹ ${Number(r["Net Taxable Parts Amt"]).toLocaleString()}</td>
</tr>
`

body.innerHTML+=row

})

}


window.onload=function(){

loadFilters()
loadCards()
loadTable()

}

</script>

</head>

<body>

<div class="container">

<h2>Renault Service Dashboard</h2>

<div class="cards">

<div class="card">
<h3>Inflow CY24</h3>
<h1 id="inflow24"></h1>
</div>

<div class="card">
<h3>Inflow CY25</h3>
<h1 id="inflow25"></h1>
</div>

<div class="card">
<h3>Total Labour</h3>
<h1 id="labour"></h1>
</div>

<div class="card">
<h3>Total Spares</h3>
<h1 id="spares"></h1>
</div>

</div>

<select id="cy">

<option value="">All CY</option>
<option value="CY24">CY24</option>
<option value="CY25">CY25</option>

</select>

<div class="filters">

<select id="division" class="multi" multiple></select>
<select id="service" class="multi" multiple></select>
<select id="sa" class="multi" multiple></select>
<select id="invoice" class="multi" multiple></select>
<select id="month" class="multi" multiple></select>

<button onclick="loadCards();loadTable()">Apply</button>

</div>

<table>

<thead>

<tr>
<th>Division</th>
<th>RO</th>
<th>SA Name</th>
<th>Vehicle</th>
<th>Service Type</th>
<th>Labour</th>
<th>Spares</th>
</tr>

</thead>

<tbody id="tbody"></tbody>

</table>

</div>

</body>
</html>

""")


# ---------------- AUTO OPEN ----------------

def open_browser():
    webbrowser.open("http://127.0.0.1:8000")


if __name__ == "__main__":
    threading.Timer(1, open_browser).start()
    uvicorn.run(app, host="127.0.0.1", port=8000)
