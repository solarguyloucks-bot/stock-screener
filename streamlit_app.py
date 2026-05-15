import streamlit as st
import yfinance as yf
import plotly.graph_objects as go
import pandas as pd
import concurrent.futures
import time
import threading
import urllib.request
import json
from datetime import datetime

# ── Globals ───────────────────────────────────────────────────────────────────
_yf_semaphore     = threading.Semaphore(3)
_cache_timestamps = {}   # ticker → datetime, persists in process memory

ANTHROPIC_API_KEY = st.secrets["ANTHROPIC_API_KEY"]

POPULAR_TICKERS = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BKNG","V","MA",
    "JPM","UNH","HD","COST","AVGO","ADBE","CRM","NFLX","SBUX","PYPL"
]

# ── Universe ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=86400)
def get_universe():
    sp500 = [
        "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB",
        "AKAM","ALB","ARE","ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN",
        "AMCR","AEE","AAL","AEP","AXP","AIG","AMT","AWK","AMP","AME","AMGN",
        "APH","ADI","ANSS","AON","APA","AAPL","AMAT","APTV","ACGL","ADM","ANET",
        "AJG","AIZ","T","ATO","ADSK","AZO","AVB","AVY","AXON","BKR","BALL","BAC",
        "BBWI","BAX","BDX","BRK-B","BBY","BIIB","BLK","BX","BA","BSX","BMY",
        "AVGO","BR","BRO","BF-B","BLDR","BG","CDNS","CZR","CPT","CPB","COF",
        "CAH","KMX","CCL","CARR","CAT","CBOE","CBRE","CDW","CE","COR","CNC",
        "CDAY","CF","CRL","SCHW","CHTR","CVX","CMG","CB","CHD","CI","CINF",
        "CTAS","CSCO","C","CFG","CLX","CME","CMS","KO","CTSH","CL","CMCSA",
        "CAG","COP","ED","STZ","CEG","COO","CPRT","GLW","CTVA","CSGP","COST",
        "CTRA","CCI","CSX","CMI","CVS","DHI","DHR","DRI","DVA","DE","DAL",
        "FANG","DLR","DFS","DG","DLTR","D","DPZ","DOV","DOW","DTE","DUK","DD",
        "EMN","ETN","EBAY","ECL","EIX","EW","EA","ELV","LLY","EMR","ENPH",
        "ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ESS","EL","ETSY","EG",
        "EVRG","ES","EXC","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO","FAST",
        "FRT","FDX","FIS","FITB","FSLR","FE","FI","FMC","F","BEN","FCX","GRMN",
        "IT","GE","GEHC","GEN","GNRC","GD","GIS","GPC","GILD","GPN","GL","GS",
        "HAL","HIG","HAS","HCA","DOC","HSIC","HSY","HES","HPE","HLT","HOLX",
        "HD","HON","HRL","HST","HWM","HPQ","HUBB","HUM","HBAN","HII","IBM",
        "IEX","IDXX","ITW","INCY","IR","PODD","INTC","ICE","IFF","IP","IPG",
        "INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT","JBL","JKHY","J","JNJ",
        "JCI","JPM","JNPR","K","KVUE","KDP","KEY","KEYS","KMB","KIM","KMI",
        "KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LIN",
        "LYV","LKQ","LMT","L","LOW","LULU","LYB","MTB","MRO","MPC","MKTX",
        "MAR","MMC","MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK",
        "META","MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","MHK","MOH",
        "TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP",
        "NFLX","NWL","NEM","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH",
        "NRG","NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE",
        "ORCL","OTIS","PCAR","PKG","PANW","PH","PAYX","PAYC","PYPL","PNR","PEP",
        "PFE","PCG","PM","PSX","PNW","PNC","POOL","PPG","PPL","PFG","PG","PGR",
        "PLD","PRU","PEG","PTC","PSA","PHM","PWR","QCOM","DGX","RL","RJF","RTX",
        "O","REG","REGN","RF","RSG","RMD","RVTY","ROK","ROL","ROP","ROST","RCL",
        "SPGI","CRM","SBAC","SLB","STX","SEE","SRE","NOW","SHW","SPG","SWKS",
        "SJM","SNA","SOLV","SO","LUV","SWK","SBUX","STT","STLD","STE","SYK",
        "SYF","SNPS","SYY","TMUS","TROW","TTWO","TPR","TRGP","TGT","TEL","TDY",
        "TFX","TER","TSLA","TXN","TXT","TMO","TJX","TSCO","TT","TDG","TRV",
        "TRMB","TFC","TYL","TSN","USB","UDR","ULTA","UNP","UAL","UPS","URI",
        "UNH","UHS","VLO","VTR","VLTO","VRSN","VRSK","VZ","VRTX","VTRS","VICI",
        "V","VMC","WRB","GWW","WAB","WBA","WMT","DIS","WBD","WM","WAT","WEC",
        "WFC","WELL","WST","WDC","WHR","WLK","WMB","WTW","WY","WYNN","XEL",
        "XYL","YUM","ZBRA","ZBH","ZTS"
    ]
    nasdaq100 = [
        "ADBE","ADP","ABNB","GOOGL","GOOG","AMZN","AMD","AEP","AMGN","ADI",
        "ANSS","AAPL","AMAT","ASML","AZN","TEAM","ADSK","BKR","BIIB","BKNG",
        "AVGO","CDNS","CDW","CHTR","CTAS","CSCO","CCEP","CTSH","CMCSA","CEG",
        "CPRT","CSGP","COST","CRWD","CSX","DDOG","DXCM","FANG","DLTR","EBAY",
        "EA","EXC","FAST","FTNT","GILD","HON","IDXX","ILMN","INTC","INTU",
        "ISRG","JD","KDP","KLAC","KHC","LRCX","LULU","MELI","META","MCHP",
        "MU","MSFT","MRNA","MDLZ","MDB","MNST","NFLX","NVDA","NXPI","ORLY",
        "ON","PCAR","PANW","PAYX","PYPL","PDD","QCOM","REGN","ROST","SBUX",
        "SNPS","TTWO","TMUS","TSLA","TXN","VRSK","VRTX","WBA","WDAY","XEL",
        "ZS","ZM"
    ]
    return list(set(sp500 + nasdaq100))

SP500_NASDAQ = get_universe()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Stock Screener", layout="wide")

st.markdown("""
<style>
    .main { background-color: #ffffff; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; max-width: 1200px; }

    .scorecard-box {
        border-radius: 10px;
        padding: 12px 8px;
        text-align: center;
        border: 1px solid;
        position: relative;
        cursor: default;
        margin-bottom: 8px;
        transition: transform 0.1s;
    }
    .scorecard-box:hover { transform: translateY(-2px); }
    .scorecard-label {
        font-size: 9px;
        font-weight: 600;
        margin: 0 0 4px 0;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .scorecard-value { font-size: 16px; font-weight: 700; margin: 0 0 4px 0; }
    .scorecard-icon  { font-size: 14px; }

    .green-box  { background: #f0faf5; border-color: #2ecc71; }
    .green-box .scorecard-label { color: #0F6E56; }
    .green-box .scorecard-value { color: #085041; }
    .green-box .scorecard-icon  { color: #2ecc71; }

    .yellow-box { background: #fffbf0; border-color: #f39c12; }
    .yellow-box .scorecard-label { color: #7d5a00; }
    .yellow-box .scorecard-value { color: #5a4000; }
    .yellow-box .scorecard-icon  { color: #f39c12; }

    .red-box    { background: #fff5f5; border-color: #e74c3c; }
    .red-box .scorecard-label { color: #922b21; }
    .red-box .scorecard-value { color: #6b1f1a; }
    .red-box .scorecard-icon  { color: #e74c3c; }

    .scorecard-box:hover .tooltip { display: block; }
    .tooltip {
        display: none;
        position: absolute;
        bottom: 110%;
        left: 50%;
        transform: translateX(-50%);
        background: #1a1a2e;
        color: #fff;
        font-size: 11px;
        padding: 8px 12px;
        border-radius: 8px;
        width: 180px;
        z-index: 999;
        line-height: 1.6;
        text-align: left;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }

    .legend-box {
        background: #f8f9fa;
        border: 1px solid #eee;
        border-radius: 10px;
        padding: 12px 16px;
        margin-bottom: 1rem;
    }
    .legend-title {
        font-size: 11px; color: #999; font-weight: 600;
        margin: 0 0 8px 0; text-transform: uppercase; letter-spacing: 0.06em;
    }
    .legend-row { display: flex; gap: 6px; flex-wrap: wrap; }
    .legend-item {
        font-size: 11px; padding: 3px 10px;
        border-radius: 20px; border: 1px solid; white-space: nowrap; font-weight: 500;
    }

    .section-header {
        font-size: 11px; color: #999; font-weight: 600;
        text-transform: uppercase; letter-spacing: 0.08em;
        margin: 1.5rem 0 0.75rem 0;
        padding-bottom: 6px;
        border-bottom: 1px solid #f0f0f0;
    }

    .ticker-name { font-size: 22px; font-weight: 700; color: #111; margin: 0; }
    .ticker-sub  { font-size: 13px; color: #888; margin: 2px 0 0 0; }

    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 14px 16px;
        border: 1px solid #eee;
    }
    .metric-label { font-size: 11px; color: #999; font-weight: 600;
                    text-transform: uppercase; letter-spacing: 0.05em; margin: 0 0 4px 0; }
    .metric-value { font-size: 20px; font-weight: 700; color: #111; margin: 0; }

    .verdict-bar {
        border-radius: 10px;
        padding: 14px 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-top: 1rem;
        border: 1px solid;
    }
    .verdict-label { font-size: 16px; font-weight: 700; }
    .verdict-count { font-size: 13px; color: #888; margin-left: 10px; }

    .stRadio > div { flex-direction: row !important; gap: 4px; }
    .stRadio > div > label {
        background: #f0f0f0; border-radius: 6px;
        padding: 4px 12px; font-size: 12px; cursor: pointer;
        border: 1px solid #ddd; font-weight: 500;
    }
    .stRadio > div > label[data-checked="true"] {
        background: #111; color: #fff; border-color: #111;
    }

    .hero-wrap { text-align: center; padding: 0.5rem 0 1rem 0; }
    .hero-title { font-size: 30px; font-weight: 700; color: #111; margin: 0; }
    .hero-sub   { font-size: 14px; color: #999; margin: 4px 0 0 0; }

    .stTextInput input {
        border-radius: 8px !important;
        border: 1px solid #ddd !important;
        font-size: 14px !important;
        padding: 10px 14px !important;
    }
    .stButton button {
        border-radius: 8px !important;
        background: #111 !important;
        color: white !important;
        font-weight: 600 !important;
        border: none !important;
        padding: 10px 20px !important;
        width: 100% !important;
    }
    .stButton button:hover { background: #333 !important; }

    div[data-testid="stHorizontalBlock"] { gap: 8px; }

    .compare-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .compare-table th {
        background: #f8f9fa; padding: 8px 12px;
        text-align: left; font-weight: 600; color: #555;
        border-bottom: 2px solid #eee;
    }
    .compare-table td { padding: 7px 12px; border-bottom: 1px solid #f5f5f5; }
    .compare-table tr:hover td { background: #fafafa; }
</style>
""", unsafe_allow_html=True)


# ── UI helpers ────────────────────────────────────────────────────────────────
def score_box(label, value_str, status, tooltip):
    icons = {"green": "✓", "yellow": "⚠", "red": "✕"}
    css   = {"green": "green-box", "yellow": "yellow-box", "red": "red-box"}
    st.markdown(f"""
        <div class="scorecard-box {css[status]}">
            <p class="scorecard-label">{label}</p>
            <p class="scorecard-value">{value_str}</p>
            <span class="scorecard-icon">{icons[status]}</span>
            <div class="tooltip">{tooltip}</div>
        </div>
    """, unsafe_allow_html=True)


def get_verdict(green_count, auto_fail):
    if auto_fail:
        return "Auto-fail", "#922b21", "#fff5f5", "#e74c3c"
    if green_count >= 8:
        return "Strong buy candidate", "#085041", "#f0faf5", "#2ecc71"
    elif green_count >= 6:
        return "Buy candidate",        "#0F6E56", "#f0faf5", "#2ecc71"
    elif green_count >= 4:
        return "Watchlist",            "#7d5a00", "#fffbf0", "#f39c12"
    elif green_count == 3:
        return "Usually pass",         "#5a4000", "#fffbf0", "#f39c12"
    else:
        return "Hard pass",            "#922b21", "#fff5f5", "#e74c3c"


def show_legend():
    st.markdown("""
        <div class="legend-box">
            <p class="legend-title">Screening legend</p>
            <div class="legend-row">
                <span class="legend-item" style="background:#f0faf5;border-color:#2ecc71;color:#085041;">8–10 ● Strong buy</span>
                <span class="legend-item" style="background:#f0faf5;border-color:#2ecc71;color:#0F6E56;">6–7 ● Buy candidate</span>
                <span class="legend-item" style="background:#fffbf0;border-color:#f39c12;color:#7d5a00;">4–5 ● Watchlist</span>
                <span class="legend-item" style="background:#fffbf0;border-color:#f39c12;color:#5a4000;">3 ● Usually pass</span>
                <span class="legend-item" style="background:#fff5f5;border-color:#e74c3c;color:#922b21;">0–2 ● Hard pass</span>
                <span class="legend-item" style="background:#fff5f5;border-color:#e74c3c;color:#922b21;">⛔ FCF red + Net debt red = Auto-fail</span>
            </div>
        </div>
    """, unsafe_allow_html=True)


# ── Claude helpers ────────────────────────────────────────────────────────────
def _claude_call(prompt, max_tokens=1000):
    try:
        data = json.dumps({
            "model": "claude-sonnet-4-5",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=data,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01"
            }
        )
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read())
            return result["content"][0]["text"]
    except Exception as e:
        return f"Unable to generate response: {str(e)}"


def get_executive_summary(ticker, name, verdict, green_count, metrics):
    metric_lines = "\n".join([f"- {label}: {val} ({status})" for label, val, status, _ in metrics])
    prompt = f"""You are a senior investment analyst at a value-focused fund. You have just run {ticker} ({name}) through a 10-metric screening framework and received these results:

{metric_lines}

Overall verdict: {verdict} ({green_count} of 10 green boxes)

Do NOT restate the metrics. Instead write 3-4 sentences of genuine analyst judgment:
- What does this combination of signals tell you about the QUALITY of this business?
- What is the key tension or trade-off (e.g. great business but expensive, or cheap but deteriorating)?
- Would you want to own this at the current price, and why?
- What would need to change to upgrade or downgrade the verdict?

Write like a seasoned analyst presenting to an investment committee. Be direct, opinionated, and specific. No bullet points, no headers, plain prose only."""
    return _claude_call(prompt, 1000)


def get_miss_explanation(ticker, name, metrics):
    weak = [(label, val, status) for label, val, status, _ in metrics if status in ("red", "yellow")]
    if not weak:
        return "All metrics passed — nothing to explain."
    lines = "\n".join([
        f"- {label}: {val} ({'FAILED' if status == 'red' else 'MARGINAL'})"
        for label, val, status in weak
    ])
    prompt = f"""Stock: {ticker} ({name})

These metrics failed or were marginal in our 10-point screening:
{lines}

For each metric above, write 2-3 sentences covering:
1. What is specifically wrong with {ticker}'s number (be quantitative)
2. Why this matters for long-term investors
3. What would realistically need to change for it to pass

Start each metric on a new line with the metric name in bold. Be direct. No preamble."""
    return _claude_call(prompt, 800)


# ── Data helpers ──────────────────────────────────────────────────────────────
def fetch_with_retry(ticker, retries=2, backoff=1.0):
    for attempt in range(retries):
        try:
            with _yf_semaphore:
                stock    = yf.Ticker(ticker)
                info     = stock.info
                if not info or len(info) < 5:
                    raise ValueError("Empty info")
                hist     = stock.history(period="max")
                income   = stock.financials
                balance  = stock.balance_sheet
                cashflow = stock.cashflow
                time.sleep(0.3)
            return stock, info, hist, income, balance, cashflow
        except Exception:
            if attempt < retries - 1:
                time.sleep(backoff)
    return None, None, None, None, None, None


@st.cache_data(ttl=86400)
def analyze(ticker):
    stock, info, hist, income, balance, cashflow = fetch_with_retry(ticker)
    if stock is None:
        raise RuntimeError(f"Could not fetch data for {ticker}")
    results = {}

    # 1. FCF Yield
    try:
        fcf = cashflow.loc["Free Cash Flow"].iloc[0]
        cap = info.get("marketCap", 0)
        fcf_yield = (fcf / cap) * 100
        results["fcf_yield_str"]    = f"{round(fcf_yield, 1)}%"
        results["fcf_yield_status"] = "green" if fcf_yield > 4 else "yellow" if fcf_yield >= 2 else "red"
        results["fcf_yield_val"]    = fcf_yield
    except:
        results["fcf_yield_str"]    = "N/A"
        results["fcf_yield_status"] = "red"
        results["fcf_yield_val"]    = None

    # 2. Revenue CAGR
    try:
        rev  = income.loc["Total Revenue"]
        cagr = ((rev.iloc[0] / rev.iloc[2]) ** (1/2) - 1) * 100
        results["rev_cagr_str"]    = f"{round(cagr, 1)}%"
        results["rev_cagr_status"] = "green" if cagr > 8 else "yellow" if cagr >= 3 else "red"
    except:
        results["rev_cagr_str"]    = "N/A"
        results["rev_cagr_status"] = "red"

    # 3. ROIC
    try:
        ebit             = income.loc["EBIT"].iloc[0]
        nopat            = ebit * (1 - 0.21)
        total_assets     = balance.loc["Total Assets"].iloc[0]
        current_liab     = balance.loc["Current Liabilities"].iloc[0]
        invested_capital = total_assets - current_liab
        roic             = (nopat / invested_capital) * 100
        results["roic_str"]    = f"{round(roic, 1)}%"
        results["roic_status"] = "green" if roic > 10 else "yellow" if roic >= 5 else "red"
    except:
        results["roic_str"]    = "N/A"
        results["roic_status"] = "red"

    # 4. Op Margin Trend
    try:
        op_income     = income.loc["Operating Income"]
        rev           = income.loc["Total Revenue"]
        margins       = (op_income / rev * 100)
        margin_change = margins.iloc[0] - margins.iloc[2]
        results["margin_trend_str"]    = f"{'+' if margin_change >= 0 else ''}{round(margin_change, 1)}%"
        results["margin_trend_status"] = "green" if margin_change >= 0 else "yellow" if margin_change >= -3 else "red"
    except:
        results["margin_trend_str"]    = "N/A"
        results["margin_trend_status"] = "red"

    # 5. Net Debt / EBITDA
    try:
        total_debt = balance.loc["Total Debt"].iloc[0]
        cash       = balance.loc["Cash And Cash Equivalents"].iloc[0]
        net_debt   = total_debt - cash
        ebitda     = info.get("ebitda", None)
        nd_ebitda  = net_debt / ebitda
        results["nd_ebitda_str"]    = f"{round(nd_ebitda, 1)}x"
        results["nd_ebitda_status"] = "green" if nd_ebitda < 2 else "yellow" if nd_ebitda <= 3.5 else "red"
        results["nd_ebitda_val"]    = nd_ebitda
    except:
        results["nd_ebitda_str"]    = "N/A"
        results["nd_ebitda_status"] = "red"
        results["nd_ebitda_val"]    = None

    # 6. FCF Conversion
    try:
        fcf_3yr        = cashflow.loc["Free Cash Flow"].iloc[:3]
        net_income_3yr = income.loc["Net Income"].iloc[:3]
        fcf_conversion = (fcf_3yr / net_income_3yr).mean() * 100
        results["fcf_conversion_str"]    = f"{round(fcf_conversion, 1)}%"
        results["fcf_conversion_status"] = "green" if fcf_conversion > 80 else "yellow" if fcf_conversion >= 50 else "red"
    except:
        results["fcf_conversion_str"]    = "N/A"
        results["fcf_conversion_status"] = "red"

    # 7. PEG
    try:
        pe = info.get("trailingPE", None)
        eg = info.get("earningsGrowth", None) or info.get("revenueGrowth", None)
        if pe and eg and eg > 0:
            peg = pe / (eg * 100)
            results["peg_str"]    = f"{round(peg, 2)}x"
            results["peg_status"] = "green" if peg < 1 else "yellow" if peg <= 2 else "red"
        else:
            net_income = income.loc["Net Income"]
            growth     = ((net_income.iloc[0] / net_income.iloc[2]) ** (1/2) - 1) * 100
            pe2        = info.get("trailingPE", None)
            if pe2 and growth > 0:
                peg = pe2 / growth
                results["peg_str"]    = f"{round(peg, 2)}x"
                results["peg_status"] = "green" if peg < 1 else "yellow" if peg <= 2 else "red"
            else:
                results["peg_str"]    = "N/A"
                results["peg_status"] = "red"
    except:
        results["peg_str"]    = "N/A"
        results["peg_status"] = "red"

    # 8. Gross Margin
    try:
        gross_profit  = income.loc["Gross Profit"]
        rev           = income.loc["Total Revenue"]
        gross_margins = (gross_profit / rev * 100)
        avg_gm        = gross_margins.mean()
        gm_change     = gross_margins.iloc[0] - gross_margins.iloc[-1]
        results["gross_margin_str"]    = f"{round(avg_gm, 1)}%"
        results["gross_margin_status"] = "green" if avg_gm > 40 and gm_change >= -2 else "yellow" if avg_gm > 20 else "red"
    except:
        results["gross_margin_str"]    = "N/A"
        results["gross_margin_status"] = "red"

    # 9. Earnings Consistency
    try:
        earnings = stock.earnings_dates
        if earnings is not None and not earnings.empty:
            col = "Surprise(%)" if "Surprise(%)" in earnings.columns else None
            if col:
                recent = earnings.dropna(subset=[col]).head(4)
                beats  = int((recent[col] > 0).sum())
            else:
                beats = 0
        else:
            q_income = stock.quarterly_financials
            net_q    = q_income.loc["Net Income"] if "Net Income" in q_income.index else None
            beats    = 0
            if net_q is not None and len(net_q) >= 4:
                beats = int(sum(net_q.iloc[i] > net_q.iloc[i+1] for i in range(min(4, len(net_q)-1))))
        results["earnings_consistency_str"]    = f"{beats}/4 beats"
        results["earnings_consistency_status"] = "green" if beats >= 3 else "yellow" if beats == 2 else "red"
    except:
        results["earnings_consistency_str"]    = "N/A"
        results["earnings_consistency_status"] = "red"

    # 10. Insider Ownership
    try:
        insider_pct = info.get("heldPercentInsiders", None)
        if insider_pct is not None:
            pct = round(insider_pct * 100, 1)
            results["insider_str"]    = f"{pct}%"
            results["insider_status"] = "green" if pct >= 5 else "yellow" if pct >= 1 else "red"
        else:
            results["insider_str"]    = "N/A"
            results["insider_status"] = "red"
    except:
        results["insider_str"]    = "N/A"
        results["insider_status"] = "red"

    # Extra: sector + earnings dates for charts
    results["sector"] = info.get("sector", "N/A")
    try:
        ed = stock.earnings_dates
        if ed is not None and not ed.empty:
            results["earnings_dates"] = [str(d.date()) for d in ed.index[:12]]
        else:
            results["earnings_dates"] = []
    except:
        results["earnings_dates"] = []

    # Record cache timestamp
    _cache_timestamps[ticker] = datetime.now()

    return info, hist, results


def scan_ticker(ticker, min_cap, min_green):
    try:
        info, hist, results = analyze(ticker)
        if info is None:
            return None
        cap = info.get("marketCap", 0)
        if cap < min_cap:
            return None
        green_count = sum(1 for k, v in results.items() if k.endswith("_status") and v == "green")
        if green_count < min_green:
            return None
        auto_fail = (results.get("fcf_yield_val") is not None and
                     results.get("nd_ebitda_val") is not None and
                     results["fcf_yield_status"] == "red" and
                     results["nd_ebitda_status"] == "red")
        verdict, _, _, _ = get_verdict(green_count, auto_fail)
        cap_str = f"${round(cap/1_000_000_000, 1)}B" if cap >= 1_000_000_000 else f"${round(cap/1_000_000, 1)}M"
        return {
            "Ticker":          ticker,
            "Company":         info.get("longName", "N/A"),
            "Sector":          results.get("sector", "N/A"),
            "Market Cap":      cap_str,
            "Green Boxes":     green_count,
            "Verdict":         verdict,
            "FCF Yield":       results["fcf_yield_str"],
            "Rev CAGR":        results["rev_cagr_str"],
            "ROIC":            results["roic_str"],
            "Op Margin":       results["margin_trend_str"],
            "Net Debt/EBITDA": results["nd_ebitda_str"],
            "FCF Conv":        results["fcf_conversion_str"],
            "PEG":             results["peg_str"],
            "Gross Margin":    results["gross_margin_str"],
            "Earnings":        results["earnings_consistency_str"],
            "Insider %":       results["insider_str"],
            "_green_raw":      green_count,
            "_cap_raw":        cap,
        }
    except:
        return None


# ── Fund helpers ──────────────────────────────────────────────────────────────
def get_fund_verdict(green_count):
    if green_count >= 8:
        return "Strong core holding", "#085041", "#f0faf5", "#2ecc71"
    elif green_count >= 6:
        return "Good holding",        "#0F6E56", "#f0faf5", "#2ecc71"
    elif green_count >= 4:
        return "Consider alternatives","#7d5a00", "#fffbf0", "#f39c12"
    elif green_count == 3:
        return "Below average",        "#5a4000", "#fffbf0", "#f39c12"
    else:
        return "Avoid",                "#922b21", "#fff5f5", "#e74c3c"


def get_fund_summary(ticker, name, verdict, green_count, metrics):
    metric_lines = "\n".join([f"- {label}: {val} ({status})" for label, val, status, _ in metrics])
    prompt = f"""You are a senior portfolio advisor at a fee-only RIA. You have just evaluated {ticker} ({name}), a Vanguard fund, using a 10-metric framework:

{metric_lines}

Overall verdict: {verdict} ({green_count} of 10 green)

Write 3-4 sentences of genuine advisor judgment — do NOT restate the metrics:
- What role does this fund play in a diversified portfolio?
- Is this a good core holding at its current cost and risk profile?
- Who is the ideal investor for this fund, and who should avoid it?
- What would change your view?

Write like an advisor presenting to a client. Be direct, plain English, no jargon. No bullet points, no headers."""
    return _claude_call(prompt, 900)


def get_fund_miss_explanation(ticker, name, metrics, fund_meta=None):
    weak = [(label, val, status) for label, val, status, _ in metrics if status in ("red", "yellow")]
    if not weak:
        return "All metrics passed — nothing to explain."
    lines = "\n".join([
        f"- {label}: {val} ({'FAILED' if status == 'red' else 'MARGINAL'})"
        for label, val, status in weak
    ])
    meta_str = ""
    if fund_meta:
        meta_str = f"""
Fund context (use this — do NOT contradict it):
- Inception: {fund_meta.get('inception', 'N/A')}
- Category: {fund_meta.get('category', 'N/A')}
- AUM: {fund_meta.get('aum', 'N/A')}
- Legal type: {fund_meta.get('legal_type', 'N/A')}
"""
    prompt = f"""Fund: {ticker} ({name}) — Vanguard
{meta_str}
Weak metrics:
{lines}

For each metric, write 2 sentences: what's wrong with the number and why it matters. Be specific and accurate — do not make assumptions that contradict the fund context above. Start each with the metric name in bold."""
    return _claude_call(prompt, 600)


@st.cache_data(ttl=86400)
def analyze_fund(ticker):
    with _yf_semaphore:
        fund   = yf.Ticker(ticker)
        info   = fund.info
        hist   = fund.history(period="max")
        time.sleep(0.3)

    if not info or len(info) < 5:
        raise RuntimeError(f"No data for {ticker}")

    # Vanguard check — fundFamily is None for mutual funds, so also check longName/shortName
    family    = info.get("fundFamily",  "") or ""
    long_name = info.get("longName",    "") or ""
    short_name= info.get("shortName",   "") or ""
    if not any("vanguard" in s.lower() for s in [family, long_name, short_name]):
        raise ValueError(f"{ticker} does not appear to be a Vanguard fund. Only Vanguard tickers are supported.")

    results = {}

    # 1. Expense Ratio
    try:
        er = info.get("annualReportExpenseRatio") or info.get("expenseRatio") or info.get("totalExpenseRatio")
        if er is not None:
            er_pct = er * 100 if er < 1 else er
            results["expense_ratio_str"]    = f"{round(er_pct, 3)}%"
            results["expense_ratio_status"] = "green" if er_pct <= 0.10 else "yellow" if er_pct <= 0.50 else "red"
            results["expense_ratio_val"]    = er_pct
        else:
            results["expense_ratio_str"]    = "N/A"
            results["expense_ratio_status"] = "yellow"
            results["expense_ratio_val"]    = None
    except:
        results["expense_ratio_str"]    = "N/A"
        results["expense_ratio_status"] = "yellow"
        results["expense_ratio_val"]    = None

    # 2. 3Y Avg Return
    try:
        r3 = info.get("threeYearAverageReturn")
        if r3 is not None:
            pct = round(r3 * 100, 2)
            results["return_3y_str"]    = f"{pct}%"
            results["return_3y_status"] = "green" if pct > 10 else "yellow" if pct >= 5 else "red"
        else:
            results["return_3y_str"]    = "N/A"
            results["return_3y_status"] = "red"
    except:
        results["return_3y_str"]    = "N/A"
        results["return_3y_status"] = "red"

    # 3. 5Y Avg Return
    try:
        r5 = info.get("fiveYearAverageReturn")
        if r5 is not None:
            pct = round(r5 * 100, 2)
            results["return_5y_str"]    = f"{pct}%"
            results["return_5y_status"] = "green" if pct > 10 else "yellow" if pct >= 5 else "red"
        else:
            results["return_5y_str"]    = "N/A"
            results["return_5y_status"] = "red"
    except:
        results["return_5y_str"]    = "N/A"
        results["return_5y_status"] = "red"

    # 4. YTD Return
    try:
        ytd = info.get("ytdReturn")
        if ytd is not None:
            pct = round(ytd * 100, 2)
            results["ytd_return_str"]    = f"{pct}%"
            results["ytd_return_status"] = "green" if pct > 0 else "yellow" if pct >= -5 else "red"
        else:
            results["ytd_return_str"]    = "N/A"
            results["ytd_return_status"] = "yellow"
    except:
        results["ytd_return_str"]    = "N/A"
        results["ytd_return_status"] = "yellow"

    # 5. Beta (3Y)
    try:
        beta = info.get("beta3Year") or info.get("beta")
        if beta is not None:
            results["beta_str"]    = f"{round(beta, 2)}"
            results["beta_status"] = "green" if 0.8 <= beta <= 1.1 else "yellow" if 0.5 <= beta <= 1.3 else "red"
            results["beta_val"]    = beta
        else:
            results["beta_str"]    = "N/A"
            results["beta_status"] = "yellow"
            results["beta_val"]    = None
    except:
        results["beta_str"]    = "N/A"
        results["beta_status"] = "yellow"
        results["beta_val"]    = None

    # 6. Turnover Rate
    try:
        turn = info.get("turnoverRatio")
        if turn is not None:
            pct = round(turn * 100, 1) if turn <= 1 else round(turn, 1)
            results["turnover_str"]    = f"{pct}%"
            results["turnover_status"] = "green" if pct < 10 else "yellow" if pct <= 50 else "red"
        else:
            results["turnover_str"]    = "N/A"
            results["turnover_status"] = "yellow"
    except:
        results["turnover_str"]    = "N/A"
        results["turnover_status"] = "yellow"

    # 7. AUM
    try:
        aum = info.get("totalAssets")
        if aum is not None:
            if aum >= 1_000_000_000:
                results["aum_str"] = f"${round(aum/1_000_000_000, 1)}B"
            else:
                results["aum_str"] = f"${round(aum/1_000_000, 1)}M"
            results["aum_status"] = "green" if aum >= 10_000_000_000 else "yellow" if aum >= 1_000_000_000 else "red"
        else:
            results["aum_str"]    = "N/A"
            results["aum_status"] = "yellow"
    except:
        results["aum_str"]    = "N/A"
        results["aum_status"] = "yellow"

    # 8. Morningstar Rating
    try:
        ms = info.get("morningStarOverallRating")
        if ms is not None:
            stars = "★" * int(ms) + "☆" * (5 - int(ms))
            results["ms_rating_str"]    = stars
            results["ms_rating_status"] = "green" if ms >= 4 else "yellow" if ms == 3 else "red"
        else:
            results["ms_rating_str"]    = "N/A"
            results["ms_rating_status"] = "yellow"
    except:
        results["ms_rating_str"]    = "N/A"
        results["ms_rating_status"] = "yellow"

    # 9. Dividend Yield
    try:
        yld = info.get("yield") or info.get("dividendYield")
        if yld is not None:
            pct = round(yld * 100, 2) if yld < 1 else round(yld, 2)
            results["div_yield_str"]    = f"{pct}%"
            results["div_yield_status"] = "green" if pct > 2 else "yellow" if pct >= 0.5 else "red"
        else:
            results["div_yield_str"]    = "N/A"
            results["div_yield_status"] = "yellow"
    except:
        results["div_yield_str"]    = "N/A"
        results["div_yield_status"] = "yellow"

    # 10. Morningstar Risk Rating
    try:
        risk = info.get("morningStarRiskRating")
        if risk is not None:
            risk_labels = {1: "Very Low", 2: "Low", 3: "Medium", 4: "High", 5: "Very High"}
            results["risk_str"]    = f"{risk} ({risk_labels.get(risk, '')})"
            results["risk_status"] = "green" if risk <= 2 else "yellow" if risk == 3 else "red"
        else:
            results["risk_str"]    = "N/A"
            results["risk_status"] = "yellow"
    except:
        results["risk_str"]    = "N/A"
        results["risk_status"] = "yellow"

    # Metadata for header
    results["category"]      = info.get("category", "N/A")
    results["fund_family"]   = info.get("fundFamily", "N/A")
    results["inception_date"]= info.get("fundInceptionDate", None)
    results["legal_type"]    = info.get("legalType", "N/A")

    _cache_timestamps[ticker] = datetime.now()
    return info, hist, results


FUND_METRIC_KEYS = [
    ("Expense ratio",      "expense_ratio"),
    ("3Y avg return",      "return_3y"),
    ("5Y avg return",      "return_5y"),
    ("YTD return",         "ytd_return"),
    ("Beta (3Y)",          "beta"),
    ("Turnover rate",      "turnover"),
    ("AUM",                "aum"),
    ("Morningstar rating", "ms_rating"),
    ("Dividend yield",     "div_yield"),
    ("Risk rating",        "risk"),
]

FUND_METRIC_TOOLTIPS = [
    "Annual fee charged by the fund. Vanguard's index funds are typically ≤0.10% — a key advantage.",
    "Average annualized return over 3 years. Above 10% is strong for diversified funds.",
    "Average annualized return over 5 years. More reliable than 3Y for long-term assessment.",
    "Return so far this calendar year. Negative YTD isn't a dealbreaker but context matters.",
    "3-year beta vs the market. 1.0 = moves with market. <1 = lower volatility. >1 = more volatile.",
    "How often the fund trades its holdings. Low turnover = lower costs and better tax efficiency.",
    "Total assets under management. Larger funds are more liquid and less likely to close.",
    "Morningstar's star rating (1–5). Based on risk-adjusted returns vs category peers.",
    "Annual income distributed as dividends. Higher yield = more income, but not always better.",
    "Morningstar's risk rating (1=Very Low, 5=Very High). Lower is safer for core holdings.",
]

# ── Pre-warm popular tickers on startup ───────────────────────────────────────
_prewarm_event = threading.Event()

def _prewarm_worker():
    for t in POPULAR_TICKERS:
        try:
            analyze(t)
        except Exception:
            pass

if not _prewarm_event.is_set():
    _prewarm_event.set()
    threading.Thread(target=_prewarm_worker, daemon=True).start()


# ── Password gate ─────────────────────────────────────────────────────────────
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.markdown("""
        <div style="max-width:360px;margin:6rem auto;text-align:center;">
            <p style="font-size:28px;font-weight:700;margin-bottom:0.25rem;">📈 Stock Screener</p>
            <p style="color:#999;font-size:14px;margin-bottom:2rem;">Enter password to continue</p>
        </div>
    """, unsafe_allow_html=True)
    col_l, col_m, col_r = st.columns([1, 2, 1])
    with col_m:
        pwd = st.text_input("Password", type="password", label_visibility="collapsed",
                            placeholder="Password")
        if pwd:
            if pwd == st.secrets["APP_PASSWORD"]:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password")
    return False

if not check_password():
    st.stop()

# ── Layout ────────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    st.image("Stonks.jpg", width=500)

st.markdown("""
    <div class="hero-wrap">
        <p class="hero-title">📈 Stock Screener</p>
        <p class="hero-sub">10-metric value investing scorecard — powered by live market data</p>
    </div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["🔍 Analyze Ticker", "🚀 Scanner", "🏦 Vanguard Funds"])

METRIC_KEYS = [
    ("FCF yield",            "fcf_yield"),
    ("3Y rev CAGR",          "rev_cagr"),
    ("ROIC",                 "roic"),
    ("Op margin trend",      "margin_trend"),
    ("Net debt/EBITDA",      "nd_ebitda"),
    ("FCF conversion",       "fcf_conversion"),
    ("PEG ratio",            "peg"),
    ("Gross margin",         "gross_margin"),
    ("Earnings consistency", "earnings_consistency"),
    ("Insider ownership",    "insider"),
]

METRIC_TOOLTIPS = [
    "Free cash flow divided by market cap. Above 4% means strong real cash relative to price. More reliable than P/E.",
    "Revenue growth compounded over 3 years. Above 8% is healthy.",
    "Return on Invested Capital. Above 10% signals a quality business.",
    "Change in operating margin over 3 years. A drop over 3% is a red flag.",
    "Leverage ratio. Below 2x is safe. Above 3.5x means the balance sheet is stretched.",
    "FCF divided by net income over 3 years. Below 50% needs explanation.",
    "P/E divided by earnings growth. Below 1.0 means you are getting growth cheaply.",
    "3-year average gross margin. Above 40% signals pricing power and a durable moat.",
    "Did the company beat estimates in 3 of last 4 quarters?",
    "Above 5% means management has real skin in the game.",
]

period_map = {"1M": "1mo", "3M": "3mo", "6M": "6mo", "1Y": "1y", "5Y": "5y", "Max": "max"}


# ── Tab 1: Analyze Ticker ─────────────────────────────────────────────────────
with tab1:
    show_legend()
    col_input, col_btn = st.columns([5, 1])
    with col_input:
        selected = st.text_input("Ticker input", placeholder="Enter ticker(s) e.g. AAPL, TSLA, BKNG ...", label_visibility="collapsed")
    with col_btn:
        analyze_btn = st.button("Analyze →", use_container_width=True)

    if analyze_btn and selected:
        st.session_state["results_data"] = {}
        tickers = [t.strip().upper() for t in selected.split(",")]
        for ticker in tickers:
            with st.spinner(f"Pulling data for {ticker}..."):
                try:
                    info, hist, results = analyze(ticker)
                    st.session_state["results_data"][ticker] = (info, hist, results)
                except Exception:
                    st.error(f"Could not load data for {ticker}. Yahoo Finance may be rate-limiting — wait a moment and try again.")

    if "results_data" in st.session_state and st.session_state["results_data"]:
        results_data = st.session_state["results_data"]
        tickers_list = list(results_data.keys())

        # ── Side-by-side comparison table (2+ tickers) ───────────────────────
        if len(tickers_list) >= 2:
            st.markdown('<p class="section-header">Side-by-side comparison</p>', unsafe_allow_html=True)
            STATUS_COLOR = {"green": "#2ecc71", "yellow": "#f39c12", "red": "#e74c3c"}

            header = "".join([f"<th>{t}</th>" for t in tickers_list])
            rows = ""
            for label, key in METRIC_KEYS:
                cells = ""
                for t in tickers_list:
                    _, _, r = results_data[t]
                    val    = r.get(f"{key}_str", "N/A")
                    status = r.get(f"{key}_status", "red")
                    color  = STATUS_COLOR[status]
                    cells += f'<td style="color:{color};font-weight:600;">{val}</td>'
                rows += f"<tr><td style='color:#666;'>{label}</td>{cells}</tr>"

            # Verdict row
            verdict_cells = ""
            for t in tickers_list:
                _, _, r = results_data[t]
                gc = sum(1 for k, v in r.items() if k.endswith("_status") and v == "green")
                af = (r.get("fcf_yield_val") is not None and r.get("nd_ebitda_val") is not None
                      and r["fcf_yield_status"] == "red" and r["nd_ebitda_status"] == "red")
                verd, vcol, _, _ = get_verdict(gc, af)
                verdict_cells += f'<td style="color:{vcol};font-weight:700;">{verd}<br><small style="color:#999;">{gc}/10 green</small></td>'
            rows += f"<tr style='background:#fafafa;'><td style='font-weight:600;'>Verdict</td>{verdict_cells}</tr>"

            # Sector row
            sector_cells = "".join([
                f'<td style="color:#555;">{results_data[t][2].get("sector","N/A")}</td>'
                for t in tickers_list
            ])
            rows += f"<tr><td style='color:#666;'>Sector</td>{sector_cells}</tr>"

            st.markdown(f"""
                <table class="compare-table">
                    <thead><tr><th>Metric</th>{header}</tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

        # ── Individual ticker analyses ────────────────────────────────────────
        for ticker, (info, hist, results) in results_data.items():
            st.markdown("---")

            name    = info.get("longName", "N/A")
            current = info.get("currentPrice", 0)
            cap     = info.get("marketCap", 0)
            cap_str = f"${round(cap/1_000_000_000, 1)}B" if cap >= 1_000_000_000 else f"${round(cap/1_000_000, 1)}M"
            high52  = info.get("fiftyTwoWeekHigh", "N/A")
            low52   = info.get("fiftyTwoWeekLow", "N/A")
            sector  = results.get("sector", "N/A")

            sector_pill = (
                f'<span style="background:#f0f4ff;color:#3a5bc7;padding:2px 10px;'
                f'border-radius:12px;font-size:12px;font-weight:600;">{sector}</span>'
                if sector != "N/A" else ""
            )
            cached_at = _cache_timestamps.get(ticker)
            cache_note = f" · cached {cached_at.strftime('%I:%M %p')}" if cached_at else ""

            st.markdown(
                f'<p class="ticker-name">{ticker} — {name}</p>'
                f'<p class="ticker-sub">{sector_pill} Live market data{cache_note}</p>',
                unsafe_allow_html=True
            )
            st.markdown("<br>", unsafe_allow_html=True)

            m1, m2, m3, m4 = st.columns(4)
            for col, label, val in [
                (m1, "Current Price", f"${current}"),
                (m2, "Market Cap", cap_str),
                (m3, "52-Week High", f"${high52}"),
                (m4, "52-Week Low", f"${low52}"),
            ]:
                col.markdown(f"""
                    <div class="metric-card">
                        <p class="metric-label">{label}</p>
                        <p class="metric-value">{val}</p>
                    </div>
                """, unsafe_allow_html=True)

            # ── Price chart with MA overlays + earnings markers ───────────────
            st.markdown('<p class="section-header">Price chart</p>', unsafe_allow_html=True)
            period_label  = st.radio("Period", list(period_map.keys()), index=1, horizontal=True, key=f"period_{ticker}")
            selected_period = period_map[period_label]

            chart_hist = yf.Ticker(ticker).history(period=selected_period)
            fig = go.Figure()

            # Price area
            fig.add_trace(go.Scatter(
                x=chart_hist.index, y=chart_hist["Close"],
                fill="tozeroy",
                line=dict(color="#2ecc71", width=2),
                fillcolor="rgba(46,204,113,0.07)",
                name="Price",
                hovertemplate="$%{y:.2f}<extra></extra>"
            ))

            # 50-day MA
            if len(chart_hist) >= 50:
                ma50 = chart_hist["Close"].rolling(50).mean()
                fig.add_trace(go.Scatter(
                    x=chart_hist.index, y=ma50,
                    line=dict(color="#f39c12", width=1.5, dash="dot"),
                    name="50-day MA",
                    hovertemplate="50MA $%{y:.2f}<extra></extra>"
                ))

            # 200-day MA
            if len(chart_hist) >= 200:
                ma200 = chart_hist["Close"].rolling(200).mean()
                fig.add_trace(go.Scatter(
                    x=chart_hist.index, y=ma200,
                    line=dict(color="#e74c3c", width=1.5, dash="dot"),
                    name="200-day MA",
                    hovertemplate="200MA $%{y:.2f}<extra></extra>"
                ))

            # Earnings date markers
            chart_start = chart_hist.index[0] if len(chart_hist) else None
            for date_str in results.get("earnings_dates", []):
                try:
                    d = datetime.strptime(date_str, "%Y-%m-%d")
                    if chart_start is not None and d >= chart_start.replace(tzinfo=None):
                        fig.add_vline(
                            x=date_str,
                            line_dash="dot",
                            line_color="rgba(90,90,180,0.45)",
                            line_width=1.5,
                            annotation_text="E",
                            annotation_position="top",
                            annotation=dict(font_size=10, font_color="rgba(90,90,180,0.7)")
                        )
                except Exception:
                    pass

            ymin = chart_hist["Close"].min() * 0.97
            ymax = chart_hist["Close"].max() * 1.03

            fig.update_layout(
                height=280,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="white",
                plot_bgcolor="white",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            font=dict(size=11)),
                xaxis=dict(showgrid=False, zeroline=False),
                yaxis=dict(showgrid=True, gridcolor="#f5f5f5", zeroline=False, range=[ymin, ymax]),
            )
            st.plotly_chart(fig, use_container_width=True)

            # ── Scorecard ─────────────────────────────────────────────────────
            st.markdown('<p class="section-header">Scorecard</p>', unsafe_allow_html=True)
            metrics = [
                (label, results[f"{key}_str"], results[f"{key}_status"], tooltip)
                for (label, key), tooltip in zip(METRIC_KEYS, METRIC_TOOLTIPS)
            ]

            row1 = st.columns(5)
            row2 = st.columns(5)
            for i, (label, val, status, tooltip) in enumerate(metrics):
                col = row1[i] if i < 5 else row2[i - 5]
                with col:
                    score_box(label, val, status, tooltip)

            st.markdown("<br>", unsafe_allow_html=True)

            green_count = sum(1 for _, _, s, _ in metrics if s == "green")
            auto_fail   = (results.get("fcf_yield_val") is not None and
                           results.get("nd_ebitda_val") is not None and
                           results["fcf_yield_status"] == "red" and
                           results["nd_ebitda_status"] == "red")
            verdict, text_color, bg_color, border_color = get_verdict(green_count, auto_fail)

            badge = (
                '<span style="font-size:12px;background:#fff5f5;color:#922b21;padding:4px 12px;'
                'border-radius:20px;border:1px solid #e74c3c;font-weight:600;">⛔ Auto-fail</span>'
                if auto_fail else
                '<span style="font-size:12px;background:#f0faf5;color:#085041;padding:4px 12px;'
                'border-radius:20px;border:1px solid #2ecc71;font-weight:600;">✓ No auto-fail</span>'
            )

            st.markdown(f"""
                <div class="verdict-bar" style="background:{bg_color};border-color:{border_color};">
                    <div style="display:flex;align-items:center;">
                        <span class="verdict-label" style="color:{text_color};">{verdict}</span>
                        <span class="verdict-count">{green_count} of 10 green</span>
                    </div>
                    {badge}
                </div>
            """, unsafe_allow_html=True)

            # ── Why did this miss? ────────────────────────────────────────────
            red_yellow = [(l, v, s) for l, v, s, _ in metrics if s in ("red", "yellow")]
            if red_yellow:
                with st.expander(f"🔍 Why did this miss? ({len(red_yellow)} weak metric{'s' if len(red_yellow) > 1 else ''})"):
                    miss_key = f"miss_{ticker}"
                    if miss_key not in st.session_state:
                        if st.button("Explain weak metrics →", key=f"miss_btn_{ticker}"):
                            with st.spinner("Analyzing..."):
                                st.session_state[miss_key] = get_miss_explanation(ticker, name, metrics)
                    if miss_key in st.session_state:
                        st.markdown(st.session_state[miss_key])

            # ── Executive Summary ─────────────────────────────────────────────
            with st.spinner("Generating executive summary..."):
                summary = get_executive_summary(ticker, name, verdict, green_count, metrics)

            st.markdown(f"""
                <p class="section-header" style="margin-top:1.5rem;">Executive summary</p>
                <div style="background:#f8f9fa;border:1px solid #eee;border-radius:10px;
                            padding:1.25rem 1.5rem;font-size:14px;line-height:1.8;color:#333;
                            margin-top:0.5rem;">
                    {summary}
                </div>
            """, unsafe_allow_html=True)


# ── Tab 2: Scanner ────────────────────────────────────────────────────────────
with tab2:
    show_legend()
    st.markdown('<p class="section-header">S&P 500 + Nasdaq 100 Scanner</p>', unsafe_allow_html=True)

    # Cache status
    cached_tickers = [t for t in SP500_NASDAQ if t in _cache_timestamps]
    if cached_tickers:
        newest = max(_cache_timestamps[t] for t in cached_tickers)
        oldest = min(_cache_timestamps[t] for t in cached_tickers)
        st.caption(
            f"📦 {len(cached_tickers)}/{len(SP500_NASDAQ)} tickers cached · "
            f"Last updated {newest.strftime('%b %d, %I:%M %p')} · "
            f"Cache refreshes at {oldest.strftime('%I:%M %p')} tomorrow"
        )
    elif _prewarm_event.is_set():
        st.caption("📦 Pre-warming cache in background (popular tickers)...")
    else:
        st.caption("📦 Cache empty — first scan will fetch all tickers fresh (~2 min)")

    st.markdown(
        '<p style="font-size:13px;color:#888;margin-bottom:1rem;">'
        'First run ~2 min · Re-runs use 24-hour cache and are near-instant.</p>',
        unsafe_allow_html=True
    )

    sc1, sc2, sc3 = st.columns([2, 2, 1])
    with sc1:
        min_cap_b = st.number_input("Min market cap ($B)", min_value=1, max_value=3000, value=50, step=5)
    with sc2:
        min_green = st.number_input("Min green boxes", min_value=1, max_value=10, value=8, step=1)
    with sc3:
        st.markdown("<br>", unsafe_allow_html=True)
        scan_btn = st.button("Run scan →", use_container_width=True, key="scan_btn")

    if scan_btn:
        min_cap      = min_cap_b * 1_000_000_000
        tickers      = list(set(SP500_NASDAQ))
        progress     = st.progress(0, text="Scanning universe...")
        results_list = []
        done         = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(scan_ticker, t, min_cap, min_green): t for t in tickers}
            for future in concurrent.futures.as_completed(futures):
                done += 1
                progress.progress(done / len(tickers), text=f"Scanning... {done}/{len(tickers)}")
                result = future.result()
                if result:
                    results_list.append(result)

        progress.empty()

        if results_list:
            df = pd.DataFrame(results_list)
            df = df.sort_values("_green_raw", ascending=False).drop(columns=["_green_raw", "_cap_raw"])
            st.success(f"Found {len(df)} stocks matching your criteria.")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("No stocks matched your criteria. Try lowering the minimum green boxes or market cap.")


# ── Tab 3: Vanguard Funds ─────────────────────────────────────────────────────
with tab3:
    st.markdown("""
        <div class="legend-box">
            <p class="legend-title">Fund screening legend</p>
            <div class="legend-row">
                <span class="legend-item" style="background:#f0faf5;border-color:#2ecc71;color:#085041;">8–10 ● Strong core holding</span>
                <span class="legend-item" style="background:#f0faf5;border-color:#2ecc71;color:#0F6E56;">6–7 ● Good holding</span>
                <span class="legend-item" style="background:#fffbf0;border-color:#f39c12;color:#7d5a00;">4–5 ● Consider alternatives</span>
                <span class="legend-item" style="background:#fffbf0;border-color:#f39c12;color:#5a4000;">3 ● Below average</span>
                <span class="legend-item" style="background:#fff5f5;border-color:#e74c3c;color:#922b21;">0–2 ● Avoid</span>
                <span class="legend-item" style="background:#f0f4ff;border-color:#3a5bc7;color:#3a5bc7;">Vanguard funds only</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown(
        '<p style="font-size:13px;color:#888;margin-bottom:1rem;">'
        'Enter a Vanguard mutual fund or ETF ticker (e.g. VFIAX, VTSAX, VOO, BND). '
        'Non-Vanguard tickers will be rejected.</p>',
        unsafe_allow_html=True
    )

    col_finput, col_fbtn = st.columns([5, 1])
    with col_finput:
        fund_selected = st.text_input("Fund ticker input", placeholder="Enter Vanguard ticker(s) e.g. VFIAX, VOO, VBTLX ...", label_visibility="collapsed")
    with col_fbtn:
        fund_btn = st.button("Analyze →", use_container_width=True, key="fund_analyze_btn")

    fund_period_map = {"1M": "1mo", "3M": "3mo", "6M": "6mo", "1Y": "1y", "5Y": "5y", "Max": "max"}

    if fund_btn and fund_selected:
        st.session_state["fund_results"] = {}
        fund_tickers = [t.strip().upper() for t in fund_selected.split(",")]
        for ft in fund_tickers:
            with st.spinner(f"Pulling fund data for {ft}..."):
                try:
                    finfo, fhist, fresults = analyze_fund(ft)
                    st.session_state["fund_results"][ft] = (finfo, fhist, fresults)
                except ValueError as e:
                    st.error(f"⛔ {e}")
                except Exception:
                    st.error(f"Could not load data for {ft}. Check the ticker and try again.")

    if "fund_results" in st.session_state and st.session_state["fund_results"]:
        fund_results_data = st.session_state["fund_results"]
        fund_tickers_list = list(fund_results_data.keys())

        # ── Side-by-side comparison (2+ funds) ───────────────────────────────
        if len(fund_tickers_list) >= 2:
            st.markdown('<p class="section-header">Side-by-side comparison</p>', unsafe_allow_html=True)
            STATUS_COLOR = {"green": "#2ecc71", "yellow": "#f39c12", "red": "#e74c3c"}

            fheader = "".join([f"<th>{t}</th>" for t in fund_tickers_list])
            frows = ""
            for label, key in FUND_METRIC_KEYS:
                cells = ""
                for ft in fund_tickers_list:
                    _, _, r = fund_results_data[ft]
                    val    = r.get(f"{key}_str", "N/A")
                    status = r.get(f"{key}_status", "yellow")
                    color  = STATUS_COLOR.get(status, "#888")
                    cells += f'<td style="color:{color};font-weight:600;">{val}</td>'
                frows += f"<tr><td style='color:#666;'>{label}</td>{cells}</tr>"

            # Verdict row
            fverdict_cells = ""
            for ft in fund_tickers_list:
                _, _, r = fund_results_data[ft]
                gc = sum(1 for k, v in r.items() if k.endswith("_status") and v == "green")
                verd, vcol, _, _ = get_fund_verdict(gc)
                fverdict_cells += f'<td style="color:{vcol};font-weight:700;">{verd}<br><small style="color:#999;">{gc}/10 green</small></td>'
            frows += f"<tr style='background:#fafafa;'><td style='font-weight:600;'>Verdict</td>{fverdict_cells}</tr>"

            # Category row
            fcat_cells = "".join([
                f'<td style="color:#555;">{fund_results_data[ft][2].get("category","N/A")}</td>'
                for ft in fund_tickers_list
            ])
            frows += f"<tr><td style='color:#666;'>Category</td>{fcat_cells}</tr>"

            st.markdown(f"""
                <table class="compare-table">
                    <thead><tr><th>Metric</th>{fheader}</tr></thead>
                    <tbody>{frows}</tbody>
                </table>
            """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

        # ── Individual fund analyses ──────────────────────────────────────────
        for ft, (finfo, fhist, fresults) in fund_results_data.items():
            st.markdown("---")

            fname     = finfo.get("longName", finfo.get("shortName", "N/A"))
            nav       = finfo.get("previousClose") or finfo.get("regularMarketPreviousClose") or finfo.get("nav", "N/A")
            aum       = fresults.get("aum_str", "N/A")
            category  = fresults.get("category", "N/A")
            legal     = fresults.get("legal_type", "N/A")
            inception = fresults.get("inception_date")
            inception_str = datetime.fromtimestamp(inception).strftime("%b %d, %Y") if inception else "N/A"
            cached_at = _cache_timestamps.get(ft)
            cache_note = f" · cached {cached_at.strftime('%I:%M %p')}" if cached_at else ""

            cat_pill = (
                f'<span style="background:#f0f4ff;color:#3a5bc7;padding:2px 10px;'
                f'border-radius:12px;font-size:12px;font-weight:600;">{category}</span>'
                if category != "N/A" else ""
            )
            vanguard_pill = (
                '<span style="background:#f0faf5;color:#085041;padding:2px 10px;'
                'border-radius:12px;font-size:12px;font-weight:600;">✓ Vanguard</span>'
            )

            st.markdown(
                f'<p class="ticker-name">{ft} — {fname}</p>'
                f'<p class="ticker-sub">{vanguard_pill} {cat_pill} {legal}{cache_note}</p>',
                unsafe_allow_html=True
            )
            st.markdown("<br>", unsafe_allow_html=True)

            # Header metrics
            m1, m2, m3, m4 = st.columns(4)
            for col, label, val in [
                (m1, "NAV / Price", f"${nav}" if nav != "N/A" else "N/A"),
                (m2, "AUM",         aum),
                (m3, "Category",    category),
                (m4, "Inception",   inception_str),
            ]:
                col.markdown(f"""
                    <div class="metric-card">
                        <p class="metric-label">{label}</p>
                        <p class="metric-value" style="font-size:15px;">{val}</p>
                    </div>
                """, unsafe_allow_html=True)

            # ── NAV Chart with MA overlays ────────────────────────────────────
            st.markdown('<p class="section-header">NAV / price chart</p>', unsafe_allow_html=True)
            fp_label  = st.radio("Period", list(fund_period_map.keys()), index=3, horizontal=True, key=f"fperiod_{ft}")
            fp_period = fund_period_map[fp_label]

            fchart_hist = yf.Ticker(ft).history(period=fp_period)
            ffig = go.Figure()

            ffig.add_trace(go.Scatter(
                x=fchart_hist.index, y=fchart_hist["Close"],
                fill="tozeroy",
                line=dict(color="#3a5bc7", width=2),
                fillcolor="rgba(58,91,199,0.07)",
                name="NAV",
                hovertemplate="$%{y:.2f}<extra></extra>"
            ))
            if len(fchart_hist) >= 50:
                fma50 = fchart_hist["Close"].rolling(50).mean()
                ffig.add_trace(go.Scatter(
                    x=fchart_hist.index, y=fma50,
                    line=dict(color="#f39c12", width=1.5, dash="dot"),
                    name="50-day MA", hovertemplate="50MA $%{y:.2f}<extra></extra>"
                ))
            if len(fchart_hist) >= 200:
                fma200 = fchart_hist["Close"].rolling(200).mean()
                ffig.add_trace(go.Scatter(
                    x=fchart_hist.index, y=fma200,
                    line=dict(color="#e74c3c", width=1.5, dash="dot"),
                    name="200-day MA", hovertemplate="200MA $%{y:.2f}<extra></extra>"
                ))

            fymin = fchart_hist["Close"].min() * 0.97
            fymax = fchart_hist["Close"].max() * 1.03
            ffig.update_layout(
                height=280,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="white", plot_bgcolor="white",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
                xaxis=dict(showgrid=False, zeroline=False),
                yaxis=dict(showgrid=True, gridcolor="#f5f5f5", zeroline=False, range=[fymin, fymax]),
            )
            st.plotly_chart(ffig, use_container_width=True)

            # ── Scorecard ─────────────────────────────────────────────────────
            st.markdown('<p class="section-header">Scorecard</p>', unsafe_allow_html=True)
            fmetrics = [
                (label, fresults.get(f"{key}_str", "N/A"), fresults.get(f"{key}_status", "yellow"), tip)
                for (label, key), tip in zip(FUND_METRIC_KEYS, FUND_METRIC_TOOLTIPS)
            ]

            frow1 = st.columns(5)
            frow2 = st.columns(5)
            for i, (label, val, status, tooltip) in enumerate(fmetrics):
                col = frow1[i] if i < 5 else frow2[i - 5]
                with col:
                    score_box(label, val, status, tooltip)

            st.markdown("<br>", unsafe_allow_html=True)

            fgreen_count = sum(1 for _, _, s, _ in fmetrics if s == "green")
            fverdict, ftext_color, fbg_color, fborder_color = get_fund_verdict(fgreen_count)

            st.markdown(f"""
                <div class="verdict-bar" style="background:{fbg_color};border-color:{fborder_color};">
                    <div style="display:flex;align-items:center;">
                        <span class="verdict-label" style="color:{ftext_color};">{fverdict}</span>
                        <span class="verdict-count">{fgreen_count} of 10 green</span>
                    </div>
                    <span style="font-size:12px;background:#f0f4ff;color:#3a5bc7;padding:4px 12px;
                          border-radius:20px;border:1px solid #3a5bc7;font-weight:600;">🏦 Vanguard</span>
                </div>
            """, unsafe_allow_html=True)

            # ── Why did this miss? ────────────────────────────────────────────
            fred_yellow = [(l, v, s) for l, v, s, _ in fmetrics if s in ("red", "yellow")]
            if fred_yellow:
                with st.expander(f"🔍 Why did this miss? ({len(fred_yellow)} weak metric{'s' if len(fred_yellow) > 1 else ''})"):
                    fmiss_key = f"fund_miss_{ft}"
                    if fmiss_key not in st.session_state:
                        if st.button("Explain weak metrics →", key=f"fund_miss_btn_{ft}"):
                            with st.spinner("Analyzing..."):
                                st.session_state[fmiss_key] = get_fund_miss_explanation(ft, fname, fmetrics, fund_meta={
                                    "inception":  inception_str,
                                    "category":   category,
                                    "aum":        aum,
                                    "legal_type": legal,
                                })
                    if fmiss_key in st.session_state:
                        st.markdown(st.session_state[fmiss_key])

            # ── Advisor Summary ───────────────────────────────────────────────
            with st.spinner("Generating advisor summary..."):
                fsummary = get_fund_summary(ft, fname, fverdict, fgreen_count, fmetrics)

            st.markdown(f"""
                <p class="section-header" style="margin-top:1.5rem;">Advisor summary</p>
                <div style="background:#f8f9fa;border:1px solid #eee;border-radius:10px;
                            padding:1.25rem 1.5rem;font-size:14px;line-height:1.8;color:#333;
                            margin-top:0.5rem;">
                    {fsummary}
                </div>
            """, unsafe_allow_html=True)
