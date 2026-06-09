import streamlit as st
import plotly
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import tenant as T

st.set_page_config(page_title="Ezz Steel | Smart Supply Chain", page_icon="\U0001f3ed", layout="wide", initial_sidebar_state="expanded")

st.markdown('''
<style>
    /* ── Main app background ── */
    .stApp { background-color: #080B10; }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background-color: #11161D;
        border-right: 1px solid #00AEEF33;
    }

    /* ── Metric values (KPI numbers) ── */
    [data-testid="stMetricValue"] { font-size: 28px !important; color: #00AEEF !important; font-weight: 700 !important; }
    [data-testid="stMetricLabel"] { color: #B6BEC8 !important; font-size: 13px !important; }
    [data-testid="stMetricDelta"] { color: #00D084 !important; }

    /* ── Headings ── */
    h1, h2, h3 { color: #C7CDD4 !important; }

    /* ── Title banner ── */
    .title-banner {
        background: #11161D;
        padding: 15px 30px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 20px;
        border: 1px solid #00AEEF44;
    }
    .title-banner h1 { color: #FFFFFF !important; margin: 0; font-size: 28px; }
    .title-banner p  { color: #B6BEC8; margin: 5px 0 0 0; font-size: 14px; }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #171E27;
        border: 1px solid #C7CDD422;
        border-radius: 6px;
        color: #8A929D;
        font-size: 13px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #00AEEF !important;
        color: #080B10 !important;
        border-color: #00AEEF !important;
    }

    /* ── Cards / containers ── */
    [data-testid="stVerticalBlock"] > div { background-color: transparent; }
    .element-container { background-color: transparent; }

    /* ── Selectbox / inputs ── */
    .stSelectbox > div > div {
        background-color: #171E27 !important;
        border-color: #C7CDD433 !important;
        color: #C7CDD4 !important;
    }
    .stRadio label { color: #B6BEC8 !important; }
    .stRadio [data-testid="stMarkdownContainer"] p { color: #B6BEC8 !important; }

    /* ── Dataframes / tables ── */
    [data-testid="stDataFrame"] { background-color: #171E27 !important; }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #11161D; }
    ::-webkit-scrollbar-thumb { background: #C7CDD444; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #00AEEF66; }
</style>
''', unsafe_allow_html=True)

@st.cache_resource
def get_engine():
    return create_engine("postgresql+psycopg2://steel_admin:steel_pass_2024@steel-postgres:5432/steel_db", 
                         pool_size=10, max_overflow=20)
# Module-level tenant scope; set by the sidebar selector below.
TENANT = {"company_id": "EZZ", "factory_id": None, "company_name": "EZZ Steel Group",
          "factory_name": "All Factories", "is_company_view": True}

def run_query(query, tenant_aware=True):
    q = T.twrap(query, TENANT) if tenant_aware else query
    try:
        return pd.read_sql(q, get_engine())
    except Exception:
        st.cache_resource.clear()
        return pd.read_sql(q, get_engine())

COLORS_SEQ = ['#00AEEF', '#3BC8FF', '#7DE2FF', '#C7CDD4', '#8A929D', '#00D084', '#FFB547', '#FF4D6D']

def apply_dark_theme(fig):
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='#11161D',
        font=dict(color='#B6BEC8', family='Arial, sans-serif', size=12),
        xaxis=dict(
            gridcolor='#C7CDD411',
            zerolinecolor='#C7CDD422',
            tickcolor='#7D8896',
            linecolor='#C7CDD422',
        ),
        yaxis=dict(
            gridcolor='#C7CDD411',
            zerolinecolor='#C7CDD422',
            tickcolor='#7D8896',
            linecolor='#C7CDD422',
        ),
        legend=dict(
            bgcolor='#171E27',
            bordercolor='#C7CDD422',
            borderwidth=1,
            font=dict(color='#B6BEC8'),
        ),
        margin=dict(l=40, r=20, t=40, b=40),
        hoverlabel=dict(
            bgcolor='#1F2833',
            bordercolor='#00AEEF',
            font=dict(color='#FFFFFF'),
        ),
    )
    return fig


with st.sidebar:
    st.markdown('''
<div style="text-align:center; padding:24px 0 16px;">
    <div style="font-size:32px; letter-spacing:2px; font-weight:700;
                color:#C7CDD4; margin-bottom:4px;">FERRO<span style="color:#00AEEF">FLUX</span></div>
    <div style="font-size:11px; color:#7D8896; letter-spacing:3px;
                text-transform:uppercase;">Steel Intelligence Platform</div>
    <div style="height:1px; background:linear-gradient(90deg,transparent,#00AEEF44,transparent);
                margin:14px 0 0;"></div>
</div>
''', unsafe_allow_html=True)
    st.markdown("---")
    globals()["TENANT"] = T.tenant_selector()
    st.markdown("---")
    page = st.radio("\U0001f4ca Navigation", [
        "\U0001f3e0 Executive Dashboard",
        "\U0001f4c8 Market & Pricing",
        "\U0001f3ed Production Analytics",
        "\U0001f4e6 Orders & Demand",
        "\U0001f69b Logistics & Procurement"
    ], index=0)
    st.markdown("---")
    st.markdown("### \U0001f4c5 Date Range")
    date_range = st.selectbox("Period", ["All Data", "Year 2024", "Year 2023", "Last 6 Months", "Last 90 Days", "Last 30 Days"], index=0)
    date_map = {"Last 30 Days": "AND date >= '2024-12-01'", "Last 90 Days": "AND date >= '2024-10-01'", "Last 6 Months": "AND date >= '2024-07-01'", "Year 2024": "AND EXTRACT(YEAR FROM date) = 2024", "Year 2023": "AND EXTRACT(YEAR FROM date) = 2023", "All Data": ""}
    date_sql = date_map[date_range]
    st.markdown("---")
    st.markdown('''<div style="text-align:center;"><p style="color:#3BC8FF; font-size:11px;">Powered by Spark | Kafka | PostgreSQL</p></div>''', unsafe_allow_html=True)


if page == "\U0001f3e0 Executive Dashboard":
    st.markdown('''<div class="title-banner"><h1>\U0001f3ed EZZ STEEL - Executive Command Center</h1><p>Real-time Supply Chain Intelligence | Powered by Big Data & AI</p></div>''', unsafe_allow_html=True)
    st.caption('\U0001f4cd ' + T.tenant_banner(TENANT))

    kpis = run_query("SELECT SUM(total_production_tons) as total_production, AVG(avg_efficiency) as avg_efficiency, SUM(total_revenue_egp) as total_revenue, SUM(total_orders) as total_orders, AVG(on_time_delivery_pct) as on_time_pct, SUM(total_co2_kg) as total_co2, SUM(profit_estimate_egp) as total_profit FROM analytics.daily_kpis")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Total Production", f"{kpis['total_production'].iloc[0]/1e6:.1f}M tons", "+2.3%")
    with c2:
        st.metric("Avg Efficiency", f"{kpis['avg_efficiency'].iloc[0]:.1f}%", "+1.2%")
    with c3:
        st.metric("Total Revenue", f"{kpis['total_revenue'].iloc[0]/1e9:.1f}B EGP", "+5.8%")
    with c4:
        st.metric("Total Orders", f"{int(kpis['total_orders'].iloc[0]):,}", "+3.1%")
    with c5:
        st.metric("On-Time", f"{kpis['on_time_pct'].iloc[0]:.1f}%", "-0.5%")

    st.markdown("---")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### Daily Production & Revenue")
        daily = run_query(f"SELECT date, total_production_tons, total_revenue_egp FROM analytics.daily_kpis WHERE 1=1 {date_sql} ORDER BY date")
        fig = make_subplots(specs=[[dict(secondary_y=True)]])
        fig.add_trace(go.Scatter(x=daily['date'], y=daily['total_production_tons'], name='Production', fill='tozeroy', line=dict(color='#00AEEF', width=1), fillcolor='rgba(0,174,239,0.2)'), secondary_y=False)
        fig.add_trace(go.Scatter(x=daily['date'], y=daily['total_revenue_egp']/1e9, name='Revenue (B EGP)', line=dict(color='#C7CDD4', width=2)), secondary_y=True)
        fig.update_yaxes(title_text="Production (tons)", secondary_y=False)
        fig.update_yaxes(title_text="Revenue (B EGP)", secondary_y=True)
        fig = apply_dark_theme(fig)
        fig.update_layout(height=350, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("### Monthly Performance")
        monthly = run_query("SELECT month_start, total_revenue_egp, gross_margin_pct FROM analytics.monthly_summary ORDER BY month_start")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly['month_start'], y=monthly['total_revenue_egp']/1e9, name='Revenue (B EGP)', marker_color='#00AEEF'))
        fig.add_trace(go.Scatter(x=monthly['month_start'], y=monthly['gross_margin_pct'], name='Margin %', yaxis='y2', line=dict(color='#C7CDD4', width=3), mode='lines+markers'))
        fig.update_layout(yaxis2=dict(title='Margin %', overlaying='y', side='right', gridcolor='rgba(74,144,217,0.1)'), yaxis=dict(title='Revenue (B EGP)'), height=350, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Supplier Risk Scorecard (AI)")
        sup = run_query("SELECT supplier_name, risk_score, risk_level FROM ml_models.supplier_risk_scores ORDER BY risk_score")
        clr = ['#00D084' if s < 34 else '#FFB547' if s < 37 else '#FF4D6D' for s in sup['risk_score']]
        fig = go.Figure(go.Bar(x=sup['risk_score'], y=sup['supplier_name'], orientation='h', marker_color=clr, text=sup['risk_score'].round(1), textposition='outside', textfont=dict(color='#B6BEC8')))
        fig.update_layout(height=400, xaxis_title='Risk Score', xaxis=dict(range=[0, 50]))
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("### Regional Demand")
        reg = run_query("SELECT governate AS governorate, region, total_orders, total_revenue_egp, delay_pct FROM analytics.regional_demand ORDER BY total_revenue_egp DESC")
        fig = px.treemap(reg, path=['region', 'governorate'], values='total_revenue_egp', color='delay_pct', color_continuous_scale=['#00D084', '#FFB547', '#FF4D6D'], color_continuous_midpoint=reg['delay_pct'].median())
        fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
        fig = apply_dark_theme(fig)
        fig.update_traces(textinfo="label+percent parent")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### AI Price Prediction (30-Day)")
    pred = run_query("SELECT target_date, predicted_price_egp, actual_price_egp, confidence_lower, confidence_upper FROM ml_models.price_predictions ORDER BY target_date")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=pred['target_date'], y=pred['confidence_upper'], fill=None, mode='lines', line_color='rgba(0,174,239,0)', showlegend=False))
    fig.add_trace(go.Scatter(x=pred['target_date'], y=pred['confidence_lower'], fill='tonexty', mode='lines', line_color='rgba(0,174,239,0)', fillcolor='rgba(0,174,239,0.15)', name='95% Confidence'))
    fig.add_trace(go.Scatter(x=pred['target_date'], y=pred['actual_price_egp'], name='Actual', line=dict(color='#C7CDD4', width=3), mode='lines+markers'))
    fig.add_trace(go.Scatter(x=pred['target_date'], y=pred['predicted_price_egp'], name='AI Prediction', line=dict(color='#FF4D6D', width=2, dash='dash'), mode='lines+markers', marker=dict(symbol='diamond')))
    fig.update_layout(height=300, yaxis_title='Price (EGP/ton)', legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig = apply_dark_theme(fig)
    st.plotly_chart(fig, use_container_width=True)


elif page == "\U0001f4c8 Market & Pricing":
    st.markdown('''<div class="title-banner"><h1>\U0001f4c8 Market Intelligence & Price Analytics</h1><p>Steel Prices | Currency | Commodities | AI Forecasting</p></div>''', unsafe_allow_html=True)
    st.caption('\U0001f4cd ' + T.tenant_banner(TENANT))
    cc = ['steel_price_egypt_egp', 'iron_ore_price_usd', 'scrap_price_usd', 'usd_egp_rate', 'brent_oil_usd', 'natural_gas_price_usd']
    cl = ['Steel', 'Iron Ore', 'Scrap', 'USD/EGP', 'Brent', 'Gas']
    market = run_query(f"""
        SELECT 
            date, 
            CAST(steel_price_egypt_egp AS FLOAT) as steel_price_egypt_egp, 
            CAST(iron_ore_price_usd AS FLOAT) as iron_ore_price_usd, 
            CAST(scrap_price_usd AS FLOAT) as scrap_price_usd, 
            CAST(usd_egp_rate AS FLOAT) as usd_egp_rate, 
            CAST(natural_gas_price_usd AS FLOAT) as natural_gas_price_usd, 
            CAST(brent_oil_usd AS FLOAT) as brent_oil_usd,
            CAST(moving_avg_7d AS FLOAT) as moving_avg_7d,
            CAST(moving_avg_30d AS FLOAT) as moving_avg_30d,
            CAST(price_volatility_7d AS FLOAT) as price_volatility_7d,
            is_price_spike
        FROM processed_data.market_clean 
        WHERE 1=1 {date_sql} 
        ORDER BY date
    """)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        lp = market['steel_price_egypt_egp'].iloc[-1]
        pp = market['steel_price_egypt_egp'].iloc[-2]
        st.metric("Steel Price", f"{lp:,.0f} EGP", f"{(lp-pp)/pp*100:+.2f}%")
    with c2:
        st.metric("USD/EGP", f"{market['usd_egp_rate'].iloc[-1]:.2f}")
    with c3:
        st.metric("Iron Ore", f"${market['iron_ore_price_usd'].iloc[-1]:.1f}")
    with c4:
        st.metric("Brent Oil", f"${market['brent_oil_usd'].iloc[-1]:.1f}")

    st.markdown("---")
    st.markdown("### Steel Price with Moving Averages")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=market['date'], y=market['steel_price_egypt_egp'], name='Daily', line=dict(color='#00AEEF', width=1), opacity=0.7))
    fig.add_trace(go.Scatter(x=market['date'], y=market['moving_avg_7d'], name='7-Day MA', line=dict(color='#C7CDD4', width=2)))
    fig.add_trace(go.Scatter(x=market['date'], y=market['moving_avg_30d'], name='30-Day MA', line=dict(color='#FF4D6D', width=2)))
    spk = market[market['is_price_spike'] == 1]
    if len(spk) > 0:
        fig.add_trace(go.Scatter(x=spk['date'], y=spk['steel_price_egypt_egp'], mode='markers', name='Spike', marker=dict(color='#FF4D6D', size=6, symbol='triangle-up')))
    fig.update_layout(height=400, yaxis_title='Price (EGP/ton)', legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig = apply_dark_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### USD/EGP Exchange Rate")
        fig = go.Figure(go.Scatter(x=market['date'], y=market['usd_egp_rate'], fill='tozeroy', line=dict(color='#00D084', width=2), fillcolor='rgba(0,208,132,0.2)'))
        fig.update_layout(height=300, yaxis_title='EGP per USD')
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.markdown("### Price Volatility (7-Day)")
        fig = go.Figure(go.Scatter(x=market['date'], y=market['price_volatility_7d'], fill='tozeroy', line=dict(color='#FF4D6D', width=2), fillcolor='rgba(255,77,109,0.2)'))
        fig.update_layout(height=300, yaxis_title='Volatility')
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        
    st.markdown("### Price Correlation Matrix")
    
    df_plot = market[cc].apply(pd.to_numeric, errors='coerce')
    cm = df_plot.corr()
    
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(cm, annot=True, cmap='RdBu_r', vmin=-1, vmax=1, fmt=".2f", ax=ax, 
                xticklabels=cl, yticklabels=cl)
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    st.pyplot(fig)

elif page == "\U0001f3ed Production Analytics":
    st.markdown('''<div class="title-banner"><h1>\U0001f3ed Production Analytics & Efficiency</h1><p>13 Production Lines | 3 Facilities | Real-time Monitoring</p></div>''', unsafe_allow_html=True)
    st.caption('\U0001f4cd ' + T.tenant_banner(TENANT))

    pe = run_query("SELECT * FROM analytics.production_efficiency ORDER BY avg_efficiency DESC")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Lines", f"{len(pe)}")
    with c2:
        st.metric("Avg Efficiency", f"{pe['avg_efficiency'].mean():.1f}%")
    with c3:
        st.metric("Total Output", f"{pe['total_output_tons'].sum()/1e6:.1f}M tons")
    with c4:
        st.metric("Avg Downtime", f"{pe['downtime_pct'].mean():.1f}%")

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Efficiency by Line")
        clr = ['#00D084' if e > 85 else '#FFB547' if e > 80 else '#FF4D6D' for e in pe['avg_efficiency']]
        fig = go.Figure(go.Bar(x=pe['avg_efficiency'], y=pe['production_line'], orientation='h', marker_color=clr, text=pe['avg_efficiency'].round(1).astype(str) + '%', textposition='outside', textfont=dict(color='#B6BEC8')))
        fig.update_layout(height=450, xaxis_title='Efficiency %', xaxis=dict(range=[75, 90]))
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.markdown("### Output by Facility")
        fac = pe.groupby('facility').agg(total=('total_output_tons', 'sum')).reset_index()
        fig = go.Figure(data=[go.Pie(labels=fac['facility'], values=fac['total'], hole=0.5, marker_colors=['#00AEEF', '#C7CDD4', '#00D084'], textinfo='label+percent', textfont=dict(color='white', size=14))])
        fig.update_layout(height=450)
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Energy Consumption")
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure(go.Bar(x=pe['production_line'], y=pe['energy_per_ton_kwh'], marker_color='#00AEEF', text=pe['energy_per_ton_kwh'].round(1), textposition='outside', textfont=dict(color='#B6BEC8')))
        fig.update_layout(height=350, yaxis_title='kWh/ton', title='Electricity per Ton')
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = go.Figure(go.Bar(x=pe['production_line'], y=pe['gas_per_ton_m3'], marker_color='#C7CDD4', text=pe['gas_per_ton_m3'].round(1), textposition='outside', textfont=dict(color='#B6BEC8')))
        fig.update_layout(height=350, yaxis_title='m3/ton', title='Gas per Ton')
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Production Line Details")
    disp = pe[['production_line', 'facility', 'line_type', 'total_output_tons', 'avg_efficiency', 'avg_quality_score', 'energy_per_ton_kwh', 'best_shift', 'downtime_pct']].copy()
    disp.columns = ['Line', 'Facility', 'Type', 'Output', 'Eff %', 'Quality', 'kWh/ton', 'Best Shift', 'Down %']
    disp['Output'] = disp['Output'].apply(lambda x: f"{x:,.0f}")
    st.dataframe(disp, use_container_width=True, hide_index=True)


elif page == "\U0001f4e6 Orders & Demand":
    st.markdown('''<div class="title-banner"><h1>\U0001f4e6 Orders & Demand Intelligence</h1><p>28,393 Orders | 24 Governorates | AI Forecasting</p></div>''', unsafe_allow_html=True)
    st.caption('\U0001f4cd ' + T.tenant_banner(TENANT))

    regional = run_query("SELECT * FROM analytics.regional_demand ORDER BY total_revenue_egp DESC")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Orders", f"{regional['total_orders'].sum():,}")
    with c2:
        st.metric("Revenue", f"{regional['total_revenue_egp'].sum()/1e9:.1f}B EGP")
    with c3:
        st.metric("Governorates", f"{len(regional)}")
    with c4:
        st.metric("Delay Rate", f"{regional['delay_pct'].mean():.1f}%")

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Revenue by Governorate (Top 15)")
        t15 = regional.head(15)
        fig = go.Figure(go.Bar(x=t15['total_revenue_egp']/1e9, y=t15['governate'], orientation='h', marker=dict(color=t15['total_revenue_egp'], colorscale=[[0, '#11161D'], [1, '#00AEEF']]), text=(t15['total_revenue_egp']/1e9).round(1).astype(str) + 'B', textposition='outside', textfont=dict(color='#B6BEC8')))
        fig.update_layout(height=500, xaxis_title='Revenue (B EGP)')
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.markdown("### Products by Region")
        rp = run_query("SELECT region, product_type, COUNT(*) as orders, SUM(quantity_tons) as tons FROM processed_data.orders_clean WHERE status != 'cancelled' GROUP BY region, product_type ORDER BY region, tons DESC")
        fig = px.sunburst(rp, path=['region', 'product_type'], values='tons', color='tons', color_continuous_scale=['#11161D', '#00AEEF', '#00AEEF', '#C7CDD4'])
        fig.update_layout(height=500)
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### AI Demand Forecast by Product")
    fc = run_query("SELECT product_type, SUM(actual_quantity_tons) as actual, SUM(predicted_quantity_tons) as predicted FROM ml_models.demand_forecasts GROUP BY product_type ORDER BY actual DESC")
    fig = go.Figure()
    fig.add_trace(go.Bar(x=fc['product_type'], y=fc['actual'], name='Actual', marker_color='#00AEEF'))
    fig.add_trace(go.Bar(x=fc['product_type'], y=fc['predicted'], name='Forecast', marker_color='#C7CDD4'))
    fig.update_layout(height=350, barmode='group', yaxis_title='Tons', legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig = apply_dark_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Customer Type Distribution")
    cust = run_query("SELECT customer_type, COUNT(*) as orders, SUM(total_value_egp) as revenue FROM processed_data.orders_clean WHERE status != 'cancelled' GROUP BY customer_type ORDER BY revenue DESC")
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure(data=[go.Pie(labels=cust['customer_type'], values=cust['revenue'], hole=0.4, marker_colors=COLORS_SEQ, textinfo='label+percent')])
        fig.update_layout(height=350, title="Revenue Share")
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = go.Figure(data=[go.Pie(labels=cust['customer_type'], values=cust['orders'], hole=0.4, marker_colors=COLORS_SEQ, textinfo='label+percent')])
        fig.update_layout(height=350, title="Order Share")
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)


elif page == "\U0001f69b Logistics & Procurement":
    st.markdown('''<div class="title-banner"><h1>\U0001f69b Logistics & Procurement Intelligence</h1><p>27,225 Shipments | 12 Suppliers | Carbon Tracking</p></div>''', unsafe_allow_html=True)
    st.caption('\U0001f4cd ' + T.tenant_banner(TENANT))

    tab1, tab2, tab3 = st.tabs(["Logistics", "Suppliers", "Sustainability"])

    with tab1:
        carrier = run_query("SELECT carrier, COUNT(*) as shipments, ROUND(AVG(cost_per_ton)::numeric, 2) as avg_cost, ROUND(AVG(transit_days)::numeric, 1) as avg_transit, ROUND(AVG(CASE WHEN delay_days > 0 THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as delay_pct, ROUND(AVG(co2_per_ton)::numeric, 1) as avg_co2 FROM processed_data.shipments_clean GROUP BY carrier ORDER BY shipments DESC")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### Carrier Performance")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=carrier['carrier'], y=carrier['avg_cost'], name='Cost/Ton', marker_color='#00AEEF'))
            fig.add_trace(go.Scatter(x=carrier['carrier'], y=carrier['delay_pct'], name='Delay %', yaxis='y2', line=dict(color='#FF4D6D', width=3), mode='lines+markers'))
            fig.update_layout(height=350, yaxis=dict(title='Cost/Ton'), yaxis2=dict(title='Delay %', overlaying='y', side='right'), legend=dict(orientation="h", yanchor="bottom", y=1.02))
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown("### Shipment Volume")
            fig = go.Figure(data=[go.Pie(labels=carrier['carrier'], values=carrier['shipments'], hole=0.5, marker_colors=COLORS_SEQ, textinfo='label+percent')])
            fig.update_layout(height=350)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(carrier, use_container_width=True, hide_index=True)

    with tab2:
        sup = run_query("SELECT * FROM ml_models.supplier_risk_scores ORDER BY risk_score")
        st.markdown("### AI Supplier Risk Assessment")
        c1, c2 = st.columns(2)
        with c1:
            best = sup.iloc[0]
            worst = sup.iloc[-1]
            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(r=[best['on_time_factor'], best['quality_factor'], best['price_factor'], 100-best['risk_score'], best['on_time_factor']], theta=['On-Time', 'Quality', 'Price', 'Safety', 'On-Time'], fill='toself', name=f"Best: {best['supplier_name']}", line_color='#00D084', fillcolor='rgba(0,208,132,0.3)'))
            fig.add_trace(go.Scatterpolar(r=[worst['on_time_factor'], worst['quality_factor'], worst['price_factor'], 100-worst['risk_score'], worst['on_time_factor']], theta=['On-Time', 'Quality', 'Price', 'Safety', 'On-Time'], fill='toself', name=f"Worst: {worst['supplier_name']}", line_color='#FF4D6D', fillcolor='rgba(255,77,109,0.3)'))
            fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100], gridcolor='rgba(0,174,239,0.2)'), bgcolor='rgba(0,0,0,0)', angularaxis=dict(gridcolor='rgba(0,174,239,0.2)')), height=400, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown("### Risk Table")
            ds = sup[['supplier_name', 'risk_score', 'risk_level', 'on_time_factor', 'quality_factor']].copy()
            ds.columns = ['Supplier', 'Risk', 'Level', 'On-Time %', 'Quality %']
            st.dataframe(ds, use_container_width=True, hide_index=True, height=350)

    with tab3:
        st.markdown("### Carbon Emissions")
        mco2 = run_query("SELECT month_start, total_co2_kg FROM analytics.monthly_summary ORDER BY month_start")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=mco2['month_start'], y=mco2['total_co2_kg']/1000, marker_color='#00D084', name='CO2'))
        ma3 = mco2['total_co2_kg'].rolling(3).mean()/1000
        fig.add_trace(go.Scatter(x=mco2['month_start'], y=ma3, name='3M Avg', line=dict(color='#FF4D6D', width=3)))
        fig.update_layout(height=350, yaxis_title='CO2 (metric tons)', legend=dict(orientation="h", yanchor="bottom", y=1.02))
        fig = apply_dark_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        total_co2 = mco2['total_co2_kg'].sum()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Total CO2", f"{total_co2/1e6:.1f}K tons")
        with c2:
            st.metric("Monthly Avg", f"{total_co2/24/1000:.0f} metric tons")
        with c3:
            tr = mco2['total_co2_kg'].pct_change().mean() * 100
            st.metric("Trend", f"{tr:+.1f}%/month")

        st.markdown("### Transport Mode Efficiency")
        tm = run_query("SELECT transport_mode, COUNT(*) as shipments, ROUND(AVG(co2_per_ton)::numeric, 1) as co2_per_ton, ROUND(AVG(cost_per_ton)::numeric, 2) as cost_per_ton, ROUND(AVG(transit_days)::numeric, 1) as transit FROM processed_data.shipments_clean GROUP BY transport_mode ORDER BY shipments DESC")
        st.dataframe(tm, use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown('''<div style="text-align:center; padding:20px;"><p style="color:#3BC8FF; font-size:12px;"><b>EZZ STEEL - Smart Supply Chain Analytics Platform</b><br>Powered by Apache Spark | Apache Kafka | PostgreSQL | PySpark MLlib | Streamlit<br>86,044 records | 3 AI Models | Real-time Streaming | Automated Pipeline<br>Graduation Project 2026</p></div>''', unsafe_allow_html=True)
