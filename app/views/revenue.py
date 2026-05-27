# src/streamlit/pages/revenue.py
import json
import urllib.request
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from components.database import query
from components.theme import CARD_CSS, PLOTLY_THEME, fmt_brl

@st.cache_data
def load_brazil_geojson():
    url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read().decode())



def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    # ── Header ──────────────────────────────────────────────
    st.markdown("""
    <div class="dash-header">
        <h1>Receita</h1>
        <p>Análise de receita, crescimento e cancelamentos</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Filtro de período ────────────────────────────────────
    st.markdown('<div class="section-title">Período</div>', unsafe_allow_html=True)

    df_periods = query("SELECT DISTINCT year, month FROM metrics_revenue ORDER BY year, month")
    periods = [f"{r['year']}-{str(r['month']).zfill(2)}" for _, r in df_periods.iterrows()]

    col1, col2 = st.columns(2)
    with col1:
        start = st.selectbox("Período inicial", periods, index=0)
    with col2:
        end = st.selectbox("Período final", periods, index=len(periods)-1)

    start_year, start_month = start.split("-")
    end_year, end_month = end.split("-")

    # ── KPIs ─────────────────────────────────────────────────
    kpis = query(f"""
        SELECT 
            SUM(total_revenue) as total_revenue,
            SUM(total_orders) as total_orders,
            SUM(total_revenue)/SUM(total_orders) as avg_ticket,
            AVG(median_order_value) as median_ticket,
            AVG(p90_order_value) as p90_ticket
        FROM metrics_revenue
        WHERE (year > {start_year} OR (year = {start_year} AND month >= {start_month}))
        AND (year < {end_year} OR (year = {end_year} AND month <= {end_month}))
    """)

    total_rev    = kpis['total_revenue'].iloc[0]
    total_orders = int(kpis['total_orders'].iloc[0])
    avg_ticket   = kpis['avg_ticket'].iloc[0]
    med_ticket   = kpis['median_ticket'].iloc[0]
    p90_ticket   = kpis['p90_ticket'].iloc[0]

    st.markdown(f"""
    <div class="kpi-grid-5">
        <div class="kpi-card blue">
            <div class="kpi-label">Receita Total</div>
            <div class="kpi-value">{fmt_brl(total_rev)}</div>
            <div class="kpi-sub">no período</div>
        </div>
        <div class="kpi-card green">
            <div class="kpi-label">Total Pedidos</div>
            <div class="kpi-value">{total_orders:,}</div>
            <div class="kpi-sub">pedidos</div>
        </div>
        <div class="kpi-card yellow">
            <div class="kpi-label">Ticket Médio</div>
            <div class="kpi-value sm">{fmt_brl(avg_ticket)}</div>
            <div class="kpi-sub">por pedido</div>
        </div>
        <div class="kpi-card purple">
            <div class="kpi-label">Ticket Mediano</div>
            <div class="kpi-value sm">{fmt_brl(med_ticket)}</div>
            <div class="kpi-sub">mediana</div>
        </div>
        <div class="kpi-card cyan">
            <div class="kpi-label">Percentil 90</div>
            <div class="kpi-value sm">{fmt_brl(p90_ticket)}</div>
            <div class="kpi-sub">top 10%</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Receita mensal + crescimento ─────────────────────────
    st.markdown('<div class="section-title">Evolução</div>', unsafe_allow_html=True)

    df = query(f"""
        SELECT year, month, total_revenue, monthly_growth_pct
        FROM metrics_revenue
        WHERE (year > {start_year} OR (year = {start_year} AND month >= {start_month}))
        AND (year < {end_year} OR (year = {end_year} AND month <= {end_month}))
        ORDER BY year, month
    """)
    df["periodo"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Receita Mensal</div>', unsafe_allow_html=True)
        fig = px.bar(df, x="periodo", y="total_revenue",
                     labels={"total_revenue": "Receita (R$)", "periodo": ""},
                     color_discrete_sequence=["#4f46e5"])
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Crescimento Mensal (%)</div>', unsafe_allow_html=True)
        colors = df["monthly_growth_pct"].apply(lambda x: "#34d399" if x >= 0 else "#f87171")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["periodo"], y=df["monthly_growth_pct"],
            marker_color=colors, marker_line_width=0, opacity=0.9
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="#4b5563", line_width=1)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Mapa + Categoria ─────────────────────────────────────
    st.markdown('<div class="section-title">Distribuição</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Receita por Estado</div>', unsafe_allow_html=True)
        df_state = query("""
            SELECT customer_state, total_revenue, total_orders
            FROM metrics_revenue_by_state ORDER BY total_revenue DESC
        """)
        geojson = load_brazil_geojson()
        fig = px.choropleth(
            df_state,
            geojson=geojson,
            locations="customer_state",
            featureidkey="properties.sigla",
            color="total_revenue",
            color_continuous_scale="Purples",
            labels={"total_revenue": "Receita (R$)", "customer_state": "Estado"},
            hover_data={"total_orders": True}
        )
        fig.update_geos(
            fitbounds="locations",
            visible=False,
            bgcolor="rgba(0,0,0,0)"
        )
        fig.update_layout(**PLOTLY_THEME, height=360,
                          coloraxis_colorbar=dict(tickfont=dict(color='#9ca3af')))
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Top 15 Categorias por Receita</div>', unsafe_allow_html=True)
        df_cat = query("""
            SELECT product_category_name_english, total_revenue
            FROM metrics_revenue_by_category ORDER BY total_revenue DESC LIMIT 15
        """)
        fig = px.bar(df_cat, x="total_revenue", y="product_category_name_english",
                     orientation="h",
                     labels={"total_revenue": "Receita (R$)", "product_category_name_english": ""},
                     color_discrete_sequence=["#7c3aed"])
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=320)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Cancelamentos ────────────────────────────────────────
    st.markdown('<div class="section-title">Cancelamentos</div>', unsafe_allow_html=True)

    df_cancel = query("""
        SELECT year, month, cancellation_rate_pct, lost_revenue
        FROM metrics_cancellations ORDER BY year, month
    """)
    df_cancel["periodo"] = df_cancel["year"].astype(str) + "-" + df_cancel["month"].astype(str).str.zfill(2)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">📉 Taxa de Cancelamento (%)</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_cancel["periodo"], y=df_cancel["cancellation_rate_pct"],
            mode='lines+markers',
            line=dict(color='#f87171', width=2),
            marker=dict(size=5, color='#f87171'),
            fill='tozeroy', fillcolor='rgba(248,113,113,0.08)'
        ))
        fig.update_layout(**PLOTLY_THEME, height=240)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">💸 Receita Perdida por Cancelamento</div>', unsafe_allow_html=True)
        fig = px.bar(df_cancel, x="periodo", y="lost_revenue",
                     labels={"lost_revenue": "Receita Perdida (R$)", "periodo": ""},
                     color_discrete_sequence=["#dc2626"])
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=240)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    if True:
        st.markdown('<div class="section-title">Dados Brutos</div>', unsafe_allow_html=True)
        df_raw = query("SELECT * FROM metrics_revenue ORDER BY year, month")
        st.dataframe(df_raw, use_container_width=True)