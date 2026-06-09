"""
Quick Data Validation Script
Checks all generated CSV files
"""

import pandas as pd
import os

print("=" * 60)
print("  DATA VALIDATION REPORT")
print("=" * 60)

path = os.path.join("data", "raw")
files = {
    'market_data.csv': {
        'key_cols': ['date', 'steel_price_egypt_egp', 'usd_egp_rate'],
        'checks': {
            'steel_price_egypt_egp': (28000, 65000),
            'usd_egp_rate': (29, 51),
            'iron_ore_price_usd': (80, 150),
        }
    },
    'production.csv': {
        'key_cols': ['date', 'facility', 'production_line', 'actual_tons'],
        'checks': {
            'efficiency_pct': (0, 100),
            'yield_loss_pct': (0, 15),
        }
    },
    'orders.csv': {
        'key_cols': ['order_id', 'order_date', 'customer_type', 'quantity_tons'],
        'checks': {
            'quantity_tons': (1, 15000),
            'price_per_ton_egp': (25000, 70000),
        }
    },
    'shipments.csv': {
        'key_cols': ['shipment_id', 'order_id', 'transport_mode'],
        'checks': {
            'distance_km': (1, 1000),
            'cost_per_ton_km_egp': (0.5, 5.0),
        }
    },
    'raw_materials.csv': {
        'key_cols': ['purchase_id', 'material_type', 'supplier_name'],
        'checks': {
            'quantity_tons': (1, 60000),
        }
    },
}

all_good = True

for filename, config in files.items():
    filepath = os.path.join(path, filename)
    print(f"\n{'─' * 60}")
    print(f"📄 {filename}")
    print(f"{'─' * 60}")

    try:
        df = pd.read_csv(filepath)
        print(f"   Rows:    {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Size:    {os.path.getsize(filepath) / 1024 / 1024:.1f} MB")

        # Check key columns exist
        missing = [c for c in config['key_cols'] if c not in df.columns]
        if missing:
            print(f"   ❌ Missing columns: {missing}")
            all_good = False
        else:
            print(f"   ✅ Key columns present")

        # Check value ranges
        for col, (min_val, max_val) in config['checks'].items():
            if col in df.columns:
                actual_min = df[col].min()
                actual_max = df[col].max()
                nulls = df[col].isnull().sum()

                status = "✅" if actual_min >= min_val * 0.8 and actual_max <= max_val * 1.2 else "⚠️"
                print(f"   {status} {col}: {actual_min:,.1f} - {actual_max:,.1f} (nulls: {nulls})")

        # Show sample unique values for key columns
        if 'facility' in df.columns:
            print(f"   📊 Facilities: {df['facility'].unique().tolist()}")
        if 'customer_type' in df.columns:
            print(f"   📊 Customer types: {df['customer_type'].nunique()}")
        if 'transport_mode' in df.columns:
            truck = len(df[df['transport_mode'] == 'truck'])
            print(f"   📊 Truck: {truck/len(df)*100:.1f}%")
        if 'material_type' in df.columns:
            print(f"   📊 Materials: {df['material_type'].unique().tolist()}")
        if 'is_ramadan' in df.columns:
            ram = df['is_ramadan'].sum()
            print(f"   📊 Ramadan days: {ram}")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        all_good = False

print(f"\n{'=' * 60}")
if all_good:
    print("  ✅ ALL DATA VALIDATED SUCCESSFULLY!")
else:
    print("  ⚠️ SOME ISSUES FOUND - CHECK ABOVE")
print(f"{'=' * 60}")