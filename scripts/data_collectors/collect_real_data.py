"""
Real Data Collector
Fetches REAL market data from public APIs
"""

import os
import pandas as pd
from datetime import datetime

# Create folder
save_path = os.path.join("data", "raw", "real_data")
os.makedirs(save_path, exist_ok=True)

print("=" * 60)
print("  REAL DATA COLLECTOR")
print("  Fetching from Public APIs...")
print("=" * 60)


# ===== 1. INSTALL REQUIRED PACKAGES =====
try:
    import yfinance as yf
    print("  yfinance: Ready")
except ImportError:
    print("  Installing yfinance...")
    os.system("pip install yfinance")
    import yfinance as yf

try:
    import requests
    print("  requests: Ready")
except ImportError:
    os.system("pip install requests")
    import requests


# ===== 2. EZZ STEEL STOCK PRICE =====
print("\n[1/6] Fetching Ezz Steel stock price...")
try:
    ezz = yf.Ticker("ESRS.CA")
    ezz_df = ezz.history(period="2y")
    if not ezz_df.empty:
        ezz_df = ezz_df.reset_index()
        ezz_df['Date'] = ezz_df['Date'].dt.strftime('%Y-%m-%d')
        ezz_df.to_csv(os.path.join(save_path, "ezz_steel_stock.csv"), index=False)
        print(f"   ✅ Ezz Steel: {len(ezz_df)} records")
        print(f"   Price range: {ezz_df['Close'].min():.1f} - {ezz_df['Close'].max():.1f} EGP")
    else:
        print("   ⚠️ No data returned for ESRS.CA")
except Exception as e:
    print(f"   ❌ Error: {e}")


# ===== 3. IRON ORE PRICES =====
print("\n[2/6] Fetching Iron Ore prices...")
try:
    # TIO = Iron Ore futures
    tickers_to_try = ["TIO=F", "GLD", "SLX"]
    iron_df = None

    # Try SLX (Steel ETF) as proxy
    slx = yf.Ticker("SLX")
    slx_df = slx.history(period="2y")
    if not slx_df.empty:
        slx_df = slx_df.reset_index()
        slx_df['Date'] = slx_df['Date'].dt.strftime('%Y-%m-%d')
        slx_df.to_csv(os.path.join(save_path, "steel_etf_slx.csv"), index=False)
        print(f"   ✅ Steel ETF (SLX): {len(slx_df)} records")
        print(f"   Price range: {slx_df['Close'].min():.1f} - {slx_df['Close'].max():.1f} USD")

    # Also get PICK (Mining ETF)
    pick = yf.Ticker("PICK")
    pick_df = pick.history(period="2y")
    if not pick_df.empty:
        pick_df = pick_df.reset_index()
        pick_df['Date'] = pick_df['Date'].dt.strftime('%Y-%m-%d')
        pick_df.to_csv(os.path.join(save_path, "mining_etf_pick.csv"), index=False)
        print(f"   ✅ Mining ETF (PICK): {len(pick_df)} records")

except Exception as e:
    print(f"   ❌ Error: {e}")


# ===== 4. BRENT OIL PRICES =====
print("\n[3/6] Fetching Brent Oil prices...")
try:
    brent = yf.Ticker("BZ=F")
    brent_df = brent.history(period="2y")
    if not brent_df.empty:
        brent_df = brent_df.reset_index()
        brent_df['Date'] = brent_df['Date'].dt.strftime('%Y-%m-%d')
        brent_df.to_csv(os.path.join(save_path, "brent_oil.csv"), index=False)
        print(f"   ✅ Brent Oil: {len(brent_df)} records")
        print(f"   Price range: {brent_df['Close'].min():.1f} - {brent_df['Close'].max():.1f} USD")
    else:
        print("   ⚠️ No Brent data, trying WTI...")
        wti = yf.Ticker("CL=F")
        wti_df = wti.history(period="2y")
        if not wti_df.empty:
            wti_df = wti_df.reset_index()
            wti_df['Date'] = wti_df['Date'].dt.strftime('%Y-%m-%d')
            wti_df.to_csv(os.path.join(save_path, "wti_oil.csv"), index=False)
            print(f"   ✅ WTI Oil: {len(wti_df)} records")
except Exception as e:
    print(f"   ❌ Error: {e}")


# ===== 5. NATURAL GAS PRICES =====
print("\n[4/6] Fetching Natural Gas prices...")
try:
    gas = yf.Ticker("NG=F")
    gas_df = gas.history(period="2y")
    if not gas_df.empty:
        gas_df = gas_df.reset_index()
        gas_df['Date'] = gas_df['Date'].dt.strftime('%Y-%m-%d')
        gas_df.to_csv(os.path.join(save_path, "natural_gas.csv"), index=False)
        print(f"   ✅ Natural Gas: {len(gas_df)} records")
        print(f"   Price range: {gas_df['Close'].min():.2f} - {gas_df['Close'].max():.2f} USD")
except Exception as e:
    print(f"   ❌ Error: {e}")


# ===== 6. USD/EGP EXCHANGE RATE =====
print("\n[5/6] Fetching USD/EGP exchange rate...")
try:
    # Method 1: Yahoo Finance
    fx = yf.Ticker("EGPUSD=X")
    fx_df = fx.history(period="2y")
    if not fx_df.empty:
        fx_df = fx_df.reset_index()
        fx_df['Date'] = fx_df['Date'].dt.strftime('%Y-%m-%d')
        # Yahoo gives EGP/USD, we need USD/EGP
        fx_df['usd_egp_rate'] = 1 / fx_df['Close']
        fx_df.to_csv(os.path.join(save_path, "usd_egp_yahoo.csv"), index=False)
        print(f"   ✅ USD/EGP (Yahoo): {len(fx_df)} records")
        print(f"   Rate range: {fx_df['usd_egp_rate'].min():.2f} - {fx_df['usd_egp_rate'].max():.2f}")
    else:
        print("   ⚠️ Yahoo FX empty, trying API...")

    # Method 2: Free API (backup)
    try:
        url = "https://api.frankfurter.app/2023-01-01..?from=USD&to=EGP"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rates = []
            for date_str, rate_dict in data.get('rates', {}).items():
                rates.append({
                    'date': date_str,
                    'usd_egp_rate': rate_dict.get('EGP', 0)
                })
            if rates:
                fx_api_df = pd.DataFrame(rates)
                fx_api_df.to_csv(os.path.join(save_path, "usd_egp_api.csv"), index=False)
                print(f"   ✅ USD/EGP (API): {len(fx_api_df)} records")
    except Exception as e2:
        print(f"   ⚠️ API backup also failed: {e2}")

except Exception as e:
    print(f"   ❌ Error: {e}")


# ===== 7. WORLD BANK COMMODITY PRICES =====
print("\n[6/6] Fetching World Bank commodity data...")
try:
    wb_url = ("https://thedocs.worldbank.org/en/doc/"
              "5d903e848db1d1b83e0ec8f744e55570-0350012021/related/"
              "CMO-Historical-Data-Monthly.xlsx")
    response = requests.get(wb_url, timeout=30)
    if response.status_code == 200:
        wb_path = os.path.join(save_path, "world_bank_commodities.xlsx")
        with open(wb_path, 'wb') as f:
            f.write(response.content)
        print(f"   ✅ World Bank data downloaded ({len(response.content)//1024} KB)")
    else:
        print(f"   ⚠️ World Bank returned status: {response.status_code}")
except Exception as e:
    print(f"   ❌ Error: {e}")


# ===== SUMMARY =====
print("\n" + "=" * 60)
print("  COLLECTION COMPLETE!")
print("=" * 60)

files = os.listdir(save_path)
print(f"  Files downloaded: {len(files)}")
for f in sorted(files):
    size = os.path.getsize(os.path.join(save_path, f))
    print(f"  📁 {f} ({size//1024} KB)")

print(f"\n  Saved to: {save_path}")
print("=" * 60)