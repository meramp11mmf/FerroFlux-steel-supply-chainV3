"""
Steel Supply Chain - Complete Data Generator V2
Calibrated with REAL Egyptian Steel Industry Data
Sources: Ezz Steel Reports, AISU, EGX, World Bank, CAPMAS
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import os


class SteelDataGenerator:

    def __init__(self):
        self.save_path = os.path.join("data", "raw")
        os.makedirs(self.save_path, exist_ok=True)
        np.random.seed(42)
        self.days_back = 730
        self.start_date = datetime(2023, 1, 1)
        self.dates = pd.date_range(
            start=self.start_date,
            periods=self.days_back,
            freq='D'
        )
        print("=" * 60)
        print("  STEEL SUPPLY CHAIN DATA GENERATOR V2")
        print("  Calibrated with Real Egyptian Industry Data")
        print("=" * 60)

    def _is_ramadan(self, date):
        ramadan = {
            2023: (datetime(2023, 3, 23), datetime(2023, 4, 21)),
            2024: (datetime(2024, 3, 12), datetime(2024, 4, 10)),
        }
        r = ramadan.get(date.year, (None, None))
        if r[0] and r[1]:
            return r[0] <= date <= r[1]
        return False

    def _get_seasonality(self, date):
        if self._is_ramadan(date):
            return np.random.uniform(0.60, 0.70)
        m = date.month
        if m in [12, 1, 2, 3]:
            return np.random.uniform(1.15, 1.30)
        elif m in [6, 7, 8]:
            return np.random.uniform(0.80, 0.90)
        else:
            return np.random.uniform(0.95, 1.05)

    def _get_usd_egp(self, date):
        if date < datetime(2024, 1, 1):
            base = 30.0 + ((date - self.start_date).days / 365)
            return round(base + np.random.normal(0, 0.15), 4)
        elif date < datetime(2024, 3, 1):
            return round(31.0 + np.random.uniform(0, 0.5), 4)
        elif date < datetime(2024, 3, 15):
            day_in = (date - datetime(2024, 3, 1)).days
            base = 31.0 + (day_in / 14) * 18.0
            return round(base + np.random.normal(0, 1.0), 4)
        else:
            return round(48.5 + np.random.uniform(-1.5, 1.5), 4)

    def _get_steel_price(self, date):
        s = self._get_seasonality(date)
        if date < datetime(2023, 7, 1):
            base = np.random.uniform(30000, 33000)
        elif date < datetime(2024, 1, 1):
            base = np.random.uniform(31000, 35000)
        elif date < datetime(2024, 2, 1):
            base = np.random.uniform(38000, 45000)
        elif date < datetime(2024, 4, 1):
            base = np.random.uniform(45000, 52000)
        elif date < datetime(2024, 7, 1):
            base = np.random.uniform(37000, 42000)
        else:
            base = np.random.uniform(34700, 40700)
        return round(max(base * s + np.random.normal(0, 500), 28000), 2)

    # ===== 1. MARKET DATA =====
    def generate_market_data(self):
        print("\n[1/5] Generating market data...")
        records = []
        for date in self.dates:
            usd = self._get_usd_egp(date)
            steel = self._get_steel_price(date)
            iron = (120.31 if date.year == 2023 else 111.06) + np.random.normal(0, 9)
            records.append({
                'date': date.strftime('%Y-%m-%d'),
                'steel_price_egypt_egp': steel,
                'iron_ore_price_usd': round(max(iron, 90), 2),
                'scrap_price_usd': round(np.random.uniform(380, 510), 2),
                'usd_egp_rate': usd,
                'natural_gas_price_usd': round(np.random.uniform(2.5, 5.5), 4),
                'brent_oil_usd': round(np.random.uniform(70, 95), 2),
                'electricity_price_egp_kwh': round(
                    np.random.uniform(1.2, 1.6) if date < datetime(2024, 3, 1)
                    else np.random.uniform(1.8, 2.5), 4),
                'seasonality_index': round(self._get_seasonality(date), 4),
                'is_ramadan': self._is_ramadan(date),
            })
        df = pd.DataFrame(records)
        df.to_csv(os.path.join(self.save_path, "market_data.csv"), index=False)
        print(f"   Done: {len(df):,} records")
        print(f"   Steel: {df['steel_price_egypt_egp'].min():,.0f}-{df['steel_price_egypt_egp'].max():,.0f} EGP")
        print(f"   USD/EGP: {df['usd_egp_rate'].min():.1f}-{df['usd_egp_rate'].max():.1f}")
        return df

    # ===== 2. PRODUCTION DATA =====
    def generate_production(self):
        print("\n[2/5] Generating production data...")
        lines = [
            ("ALEX_DRI_01", "ALEX", "DRI", "sponge_iron", 7500),
            ("ALEX_DRI_02", "ALEX", "DRI", "sponge_iron", 5000),
            ("ALEX_EAF_01", "ALEX", "EAF", "billets", 5000),
            ("ALEX_EAF_02", "ALEX", "EAF", "billets", 4000),
            ("ALEX_RM_01", "ALEX", "Rolling_Mill", "rebar", 4500),
            ("ALEX_RM_02", "ALEX", "Rolling_Mill", "rebar", 3000),
            ("SUEZ_DRI_01", "SUEZ", "DRI", "sponge_iron", 6060),
            ("SUEZ_EAF_01", "SUEZ", "EAF", "billets", 4000),
            ("SUEZ_HSM_01", "SUEZ", "Hot_Strip_Mill", "HRC", 4000),
            ("SUEZ_CRM_01", "SUEZ", "Cold_Rolling", "CRC", 2000),
            ("SADAT_EAF_01", "SADAT", "EAF", "billets", 3000),
            ("SADAT_RM_01", "SADAT", "Rolling_Mill", "rebar", 1440),
            ("SADAT_RM_02", "SADAT", "Rolling_Mill", "wire_rod", 1920),
        ]
        shift_eff = {'morning': 0.92, 'afternoon': 0.87, 'night': 0.83}
        energy_rates = {'DRI': 550, 'EAF': 626, 'Rolling_Mill': 200,
                       'Hot_Strip_Mill': 250, 'Cold_Rolling': 180}
        gas_rates = {'DRI': 280, 'EAF': 15, 'Rolling_Mill': 45,
                    'Hot_Strip_Mill': 55, 'Cold_Rolling': 10}

        records = []
        for date in self.dates:
            seas = self._get_seasonality(date)
            is_summer = date.month in [6, 7, 8]
            blackout = is_summer and np.random.random() < 0.03

            for lid, fac, ltype, prod, cap in lines:
                if np.random.random() < 0.05:
                    records.append({
                        'batch_id': f"B-{str(uuid.uuid4())[:6]}",
                        'date': date.strftime('%Y-%m-%d'),
                        'facility': fac, 'production_line': lid,
                        'line_type': ltype, 'product_type': prod,
                        'shift': 'maintenance', 'planned_tons': cap,
                        'actual_tons': 0, 'waste_tons': 0,
                        'yield_loss_pct': 0, 'efficiency_pct': 0,
                        'energy_kwh': 0, 'natural_gas_m3': 0,
                        'quality_score': 0, 'status': 'maintenance',
                    })
                    continue
                if blackout:
                    records.append({
                        'batch_id': f"B-{str(uuid.uuid4())[:6]}",
                        'date': date.strftime('%Y-%m-%d'),
                        'facility': fac, 'production_line': lid,
                        'line_type': ltype, 'product_type': prod,
                        'shift': 'power_outage', 'planned_tons': cap,
                        'actual_tons': 0, 'waste_tons': 0,
                        'yield_loss_pct': 0, 'efficiency_pct': 0,
                        'energy_kwh': 0, 'natural_gas_m3': 0,
                        'quality_score': 0, 'status': 'power_outage',
                    })
                    continue
                for shift in ['morning', 'afternoon', 'night']:
                    sc = cap / 3
                    eff = np.clip(shift_eff[shift] * seas + np.random.normal(0, 0.03), 0.5, 0.98)
                    actual = sc * eff
                    yl = np.random.uniform(0.08, 0.12)
                    waste = actual * yl
                    net = actual - waste
                    eng = actual * energy_rates.get(ltype, 300) * np.random.uniform(0.92, 1.08)
                    gas = actual * gas_rates.get(ltype, 30) * np.random.uniform(0.90, 1.10)
                    q = np.random.uniform(7.0, 10.0)
                    if shift == 'afternoon': q -= np.random.uniform(0, 0.5)
                    if shift == 'night': q -= np.random.uniform(0, 0.3)
                    records.append({
                        'batch_id': f"B-{str(uuid.uuid4())[:6]}",
                        'date': date.strftime('%Y-%m-%d'),
                        'facility': fac, 'production_line': lid,
                        'line_type': ltype, 'product_type': prod,
                        'shift': shift, 'planned_tons': round(sc, 2),
                        'actual_tons': round(net, 2),
                        'waste_tons': round(waste, 2),
                        'yield_loss_pct': round(yl * 100, 2),
                        'efficiency_pct': round(eff * 100, 2),
                        'energy_kwh': round(eng, 2),
                        'natural_gas_m3': round(gas, 2),
                        'quality_score': round(max(q, 6.0), 2),
                        'status': 'running',
                    })
        df = pd.DataFrame(records)
        df.to_csv(os.path.join(self.save_path, "production.csv"), index=False)
        print(f"   Done: {len(df):,} records | Lines: {len(lines)}")
        return df
    
        # ===== 3. ORDERS DATA =====
    def generate_orders(self):
        print("\n[3/5] Generating orders...")
        govs = {
            'Cairo': 0.18, 'Giza': 0.08, 'New_Capital': 0.10,
            'Alexandria': 0.09, 'Sharqia': 0.05, 'Qalyubia': 0.05,
            'Dakahlia': 0.04, 'Gharbia': 0.03, 'Beheira': 0.03,
            'Monufia': 0.03, 'Suez_Canal_Zone': 0.06, 'Ismailia': 0.03,
            'Minya': 0.03, 'Assiut': 0.03, 'Fayoum': 0.02,
            'Sohag': 0.02, 'Qena': 0.01, 'Luxor': 0.01,
            'Aswan': 0.02, 'Red_Sea': 0.02, 'New_Alamein': 0.04,
            'Matrouh': 0.01, 'New_Valley': 0.01, 'Port_Said': 0.01,
        }
        ctypes = ['government_mega_project', 'private_contractor',
                  'distributor', 'small_contractor',
                  'industrial_manufacturer', 'export']
        cprobs = [0.12, 0.20, 0.30, 0.18, 0.08, 0.12]
        products = ['rebar_10mm', 'rebar_12mm', 'rebar_16mm',
                   'rebar_20mm', 'rebar_25mm', 'wire_rod', 'HRC', 'CRC']
        pprobs = [0.08, 0.20, 0.25, 0.12, 0.08, 0.07, 0.12, 0.08]
        qty_map = {
            'government_mega_project': (500, 5000),
            'private_contractor': (50, 500),
            'distributor': (25, 1000),
            'small_contractor': (10, 100),
            'industrial_manufacturer': (100, 2000),
            'export': (1000, 10000),
        }
        pay_map = {
            'government_mega_project': (['LC_90', 'credit_60'], [0.6, 0.4]),
            'private_contractor': (['credit_30', 'credit_60', 'cash'], [0.4, 0.35, 0.25]),
            'distributor': (['credit_30', 'credit_60', 'cash'], [0.35, 0.40, 0.25]),
            'small_contractor': (['cash', 'credit_30'], [0.6, 0.4]),
            'industrial_manufacturer': (['credit_60', 'LC_90'], [0.6, 0.4]),
            'export': (['LC_90', 'credit_60'], [0.7, 0.3]),
        }
        delays = [0, 0, 0, 0, 1, 2, 3, 5, 7]
        dprobs = [0.55, 0.10, 0.05, 0.05, 0.08, 0.07, 0.05, 0.03, 0.02]

        records = []
        for date in self.dates:
            seas = self._get_seasonality(date)
            n = int(max(15, 40 * seas + np.random.randint(-5, 5)))
            sp = self._get_steel_price(date)

            for _ in range(n):
                ct = np.random.choice(ctypes, p=cprobs)
                gov = np.random.choice(list(govs.keys()), p=list(govs.values()))
                prod = np.random.choice(products, p=pprobs)
                qty = np.random.uniform(*qty_map[ct])
                price = sp + np.random.uniform(-500, 1000)
                terms, tprobs = pay_map[ct]
                pay = np.random.choice(terms, p=tprobs)
                lt = np.random.randint(14, 30) if ct == 'export' else np.random.randint(1, 7)
                delay = np.random.choice(delays, p=dprobs)
                exp_d = date + timedelta(days=int(lt))
                act_d = date + timedelta(days=int(lt) + int(delay))

                records.append({
                    'order_id': f"ORD-{str(uuid.uuid4())[:6]}",
                    'order_date': date.strftime('%Y-%m-%d'),
                    'customer_id': f"C-{np.random.randint(1, 800):04d}",
                    'customer_type': ct,
                    'product_type': prod,
                    'rebar_size_mm': prod.split('_')[1] if 'rebar' in prod else 'N/A',
                    'quantity_tons': round(qty, 2),
                    'price_per_ton_egp': round(max(price, 28000), 2),
                    'total_value_egp': round(qty * max(price, 28000), 2),
                    'delivery_governorate': gov,
                    'expected_delivery': exp_d.strftime('%Y-%m-%d'),
                    'actual_delivery': act_d.strftime('%Y-%m-%d'),
                    'delay_days': delay,
                    'is_delayed': delay > 0,
                    'payment_terms': pay,
                    'status': np.random.choice(['delivered', 'cancelled'], p=[0.96, 0.04]),
                })

        df = pd.DataFrame(records)
        df.to_csv(os.path.join(self.save_path, "orders.csv"), index=False)
        print(f"   Done: {len(df):,} records | Govs: {len(govs)}")
        return df

    # ===== 4. SHIPMENTS DATA =====
    def generate_shipments(self, orders_df):
        print("\n[4/5] Generating shipments...")
        dists = {
            'Cairo': 130, 'Giza': 145, 'New_Capital': 160,
            'Alexandria': 350, 'Sharqia': 180, 'Qalyubia': 150,
            'Dakahlia': 230, 'Gharbia': 250, 'Beheira': 320,
            'Monufia': 200, 'Suez_Canal_Zone': 30, 'Ismailia': 80,
            'Minya': 450, 'Assiut': 550, 'Fayoum': 250,
            'Sohag': 650, 'Qena': 750, 'Luxor': 800,
            'Aswan': 950, 'Red_Sea': 300, 'New_Alamein': 400,
            'Matrouh': 550, 'New_Valley': 700, 'Port_Said': 160,
        }
        carriers = ['Nile_Logistics', 'Egyptian_Transport', 'Delta_Freight',
                    'Suez_Carriers', 'Cairo_Express', 'Upper_Egypt_Freight',
                    'Canal_Logistics', 'Pharaoh_Transport']

        delivered = orders_df[orders_df['status'] == 'delivered'].copy()
        records = []

        for _, order in delivered.iterrows():
            dist = dists.get(order['delivery_governorate'], 200)

            # 99% truck (real Egyptian data)
            if np.random.random() < 0.99:
                mode = 'truck'
                n_trucks = max(1, int(np.ceil(order['quantity_tons'] / 25)))
            else:
                mode = 'rail'
                n_trucks = 0

            # Cost: 2.50-4.50 EGP per ton-km (real data)
            cpt = np.random.uniform(2.50, 4.50) if mode == 'truck' else np.random.uniform(0.80, 1.50)
            cost = dist * order['quantity_tons'] * cpt

            # Fuel and CO2
            fuel = dist * max(n_trucks, 1) * 0.35
            co2 = fuel * 2.68

            # Port delay for exports
            port_delay = np.random.randint(0, 5) if order['customer_type'] == 'export' else 0

            records.append({
                'shipment_id': f"SHP-{str(uuid.uuid4())[:6]}",
                'order_id': order['order_id'],
                'origin': 'Factory_Suez',
                'destination': order['delivery_governorate'],
                'distance_km': dist,
                'transport_mode': mode,
                'num_trucks': n_trucks,
                'weight_tons': order['quantity_tons'],
                'transport_cost_egp': round(cost, 2),
                'cost_per_ton_km_egp': round(cpt, 2),
                'fuel_liters': round(fuel, 2),
                'co2_emissions_kg': round(co2, 2),
                'departure_date': order['order_date'],
                'arrival_date': order['actual_delivery'],
                'delay_days': order['delay_days'],
                'port_delay_days': port_delay,
                'status': 'on_time' if order['delay_days'] == 0 else 'delayed',
                'carrier': np.random.choice(carriers),
            })

        df = pd.DataFrame(records)
        df.to_csv(os.path.join(self.save_path, "shipments.csv"), index=False)
        truck_pct = len(df[df['transport_mode'] == 'truck']) / len(df) * 100
        print(f"   Done: {len(df):,} records | Truck: {truck_pct:.1f}%")
        return df

    # ===== 5. RAW MATERIALS =====
    def generate_raw_materials(self):
        print("\n[5/5] Generating raw materials...")
        suppliers = [
            ('Vale_SA', 'Brazil', 'iron_ore', 35, 0.92, 'Dekheila'),
            ('Rio_Tinto', 'Australia', 'iron_ore', 30, 0.95, 'Ain_Sokhna'),
            ('BHP', 'Australia', 'iron_ore', 30, 0.93, 'Ain_Sokhna'),
            ('FMG', 'Australia', 'iron_ore', 28, 0.90, 'Ain_Sokhna'),
            ('Local_Scrap_Cairo', 'Egypt', 'scrap', 2, 0.78, 'N/A'),
            ('Local_Scrap_Alex', 'Egypt', 'scrap', 2, 0.80, 'N/A'),
            ('Local_Scrap_Delta', 'Egypt', 'scrap', 3, 0.75, 'N/A'),
            ('Turkish_HMS', 'Turkey', 'scrap', 8, 0.85, 'Dekheila'),
            ('European_Scrap', 'Europe', 'scrap', 12, 0.88, 'Dekheila'),
            ('Coal_India', 'India', 'coal', 25, 0.88, 'Ain_Sokhna'),
            ('Limestone_Egypt', 'Egypt', 'limestone', 1, 0.92, 'N/A'),
            ('Electrode_China', 'China', 'graphite_electrodes', 30, 0.85, 'Ain_Sokhna'),
        ]
        freq_map = {'iron_ore': 0.06, 'scrap': 0.45, 'coal': 0.04,
                    'limestone': 0.4, 'graphite_electrodes': 0.02}
        qty_map = {'iron_ore': (25000, 55000), 'scrap': (200, 3000),
                   'coal': (5000, 15000), 'limestone': (500, 2000),
                   'graphite_electrodes': (50, 200)}
        price_map = {'iron_ore': (115, 12), 'scrap': (420, 35),
                     'coal': (150, 20), 'limestone': (15, 3),
                     'graphite_electrodes': (3500, 300)}
        # Ocean freight: \$8.20-\$21.65/ton (real Capesize rates)
        freight_map = {'Brazil': (12, 21.65), 'Australia': (8.20, 16),
                       'Turkey': (5, 10), 'Europe': (6, 12),
                       'India': (8, 15), 'China': (10, 18),
                       'Egypt': (0, 0)}

        records = []
        for date in self.dates:
            for name, country, mat, lead, rel, port in suppliers:
                if np.random.random() < freq_map.get(mat, 0.1):
                    qty = np.random.uniform(*qty_map[mat])
                    base_p, std_p = price_map[mat]
                    price = max(base_p + np.random.normal(0, std_p), 10)

                    # Ocean freight cost
                    fr_min, fr_max = freight_map.get(country, (5, 15))
                    freight = np.random.uniform(fr_min, fr_max) if country != 'Egypt' else 0

                    # Lead time: sailing + port handling
                    # Real: Brazil 25-35 sailing + 10-15 port = 40-45 total
                    if country in ['Brazil']:
                        total_lead = lead + np.random.randint(5, 15)
                    elif country in ['Australia']:
                        total_lead = lead + np.random.randint(5, 12)
                    elif country == 'Egypt':
                        total_lead = lead + np.random.randint(0, 2)
                    else:
                        total_lead = lead + np.random.randint(3, 8)

                    actual_lead = total_lead + np.random.randint(-3, 7)

                    records.append({
                        'purchase_id': f"PUR-{str(uuid.uuid4())[:6]}",
                        'material_type': mat,
                        'supplier_name': name,
                        'origin_country': country,
                        'quantity_tons': round(qty, 2),
                        'price_per_ton_usd': round(price, 2),
                        'ocean_freight_usd_per_ton': round(freight, 2),
                        'total_material_cost_usd': round(qty * price, 2),
                        'total_freight_cost_usd': round(qty * freight, 2),
                        'total_landed_cost_usd': round(qty * (price + freight), 2),
                        'purchase_date': date.strftime('%Y-%m-%d'),
                        'expected_delivery': (date + timedelta(days=int(total_lead))).strftime('%Y-%m-%d'),
                        'actual_delivery': (date + timedelta(days=int(actual_lead))).strftime('%Y-%m-%d'),
                        'discharge_port': port,
                        'quality_grade': np.random.choice(['A', 'B', 'C'], p=[0.6, 0.3, 0.1]),
                        'on_time': actual_lead <= total_lead,
                        'supplier_reliability': rel,
                        'dri_scrap_ratio': '80:20',
                    })

        df = pd.DataFrame(records)
        df.to_csv(os.path.join(self.save_path, "raw_materials.csv"), index=False)
        print(f"   Done: {len(df):,} records | Suppliers: {len(suppliers)}")
        return df

    # ===== GENERATE ALL =====
    def generate_all(self):
        start = datetime.now()

        market = self.generate_market_data()
        production = self.generate_production()
        orders = self.generate_orders()
        shipments = self.generate_shipments(orders)
        raw_mat = self.generate_raw_materials()

        total = sum(len(d) for d in [market, production, orders, shipments, raw_mat])
        elapsed = (datetime.now() - start).seconds

        print("\n" + "=" * 60)
        print("  GENERATION COMPLETE!")
        print("=" * 60)
        print(f"  Market Data:    {len(market):>10,}")
        print(f"  Production:     {len(production):>10,}")
        print(f"  Orders:         {len(orders):>10,}")
        print(f"  Shipments:      {len(shipments):>10,}")
        print(f"  Raw Materials:  {len(raw_mat):>10,}")
        print(f"  {'='*35}")
        print(f"  TOTAL:          {total:>10,}")
        print(f"  Time:           {elapsed} seconds")
        print(f"  Saved to:       {self.save_path}")
        print("=" * 60)

        return {
            'market': market, 'production': production,
            'orders': orders, 'shipments': shipments,
            'raw_materials': raw_mat,
        }


if __name__ == "__main__":
    gen = SteelDataGenerator()
    data = gen.generate_all()

    