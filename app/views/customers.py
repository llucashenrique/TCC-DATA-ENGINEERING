# src/streamlit/pages/customers.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from components.database import query
from components.theme import CARD_CSS, PLOTLY_THEME, fmt_brl


def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    st.markdown("""
    <div class="dash-header">
        <h1>Clientes</h1>
        <p>Aquisição, LTV e análise de retenção por cohort</p>
    </div>
    """, unsafe_allow_html=True)

    # ── KPIs ─────────────────────────────────────────────────
    kpis = query("""
        SELECT SUM(new_customers) as total_customers, AVG(avg_ltv) as avg_ltv
        FROM metrics_customers
    """)
    recurrent = query("""
        SELECT COUNT(*) as total FROM (
            SELECT customer_id FROM fact_orders
            GROUP BY customer_id HAVING COUNT(*) > 1
        ) r
    """)
    total = query("SELECT COUNT(DISTINCT customer_id) as total FROM fact_orders")

    total_customers = int(kpis['total_customers'].iloc[0])
    avg_ltv         = kpis['avg_ltv'].iloc[0]
    rec             = int(recurrent["total"].iloc[0])
    tot             = int(total["total"].iloc[0])
    rate            = round(rec / tot * 100, 1) if tot > 0 else 0

    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card blue">
            <div class="kpi-label">Total Clientes</div>
            <div class="kpi-value">{total_customers:,}</div>
            <div class="kpi-sub">únicos no período</div>
        </div>
        <div class="kpi-card yellow">
            <div class="kpi-label">LTV Médio</div>
            <div class="kpi-value">{fmt_brl(avg_ltv)}</div>
            <div class="kpi-sub">lifetime value</div>
        </div>
        <div class="kpi-card green">
            <div class="kpi-label">Clientes Recorrentes</div>
            <div class="kpi-value">{rec:,}</div>
            <div class="kpi-sub">2+ pedidos</div>
        </div>
        <div class="kpi-card purple">
            <div class="kpi-label">Taxa de Recompra</div>
            <div class="kpi-value">{rate}%</div>
            <div class="kpi-sub">dos clientes voltaram</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Aquisição + LTV ──────────────────────────────────────
    st.markdown('<div class="section-title">Aquisição</div>', unsafe_allow_html=True)

    df = query("""
        SELECT year, month, new_customers, avg_ltv
        FROM metrics_customers ORDER BY year, month
    """)
    df["periodo"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Novos Clientes por Mês</div>', unsafe_allow_html=True)
        fig = px.bar(df, x="periodo", y="new_customers",
                     labels={"new_customers": "Novos Clientes", "periodo": ""},
                     color_discrete_sequence=["#4f46e5"])
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">LTV Médio por Mês</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["periodo"], y=df["avg_ltv"],
            mode='lines+markers',
            line=dict(color='#fbbf24', width=2),
            marker=dict(size=5, color='#fbbf24'),
            fill='tozeroy', fillcolor='rgba(251,191,36,0.08)'
        ))
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Cohort ───────────────────────────────────────────────
    st.markdown('<div class="section-title">Cohort Analysis</div>', unsafe_allow_html=True)

    df_cohort = query("""
        SELECT cohort_month, months_since_first, customers
        FROM metrics_cohort ORDER BY cohort_month, months_since_first
    """)

    cohort_pivot = df_cohort.pivot(
        index="cohort_month", columns="months_since_first", values="customers"
    )
    cohort_pct = cohort_pivot.div(cohort_pivot[0], axis=0) * 100

    st.markdown('<div class="chart-card"><div class="chart-title">Taxa de Retenção por Cohort (%)</div>', unsafe_allow_html=True)
    fig = px.imshow(
        cohort_pct, text_auto=".1f",
        color_continuous_scale=["#1a1a2e", "#4f46e5", "#818cf8"],
        labels={"x": "Meses desde primeira compra", "y": "Cohort", "color": "Retenção (%)"}
    )
    fig.update_layout(**PLOTLY_THEME, height=500,
                      coloraxis_colorbar=dict(tickfont=dict(color='#9ca3af')))
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if True:
        st.markdown('<div class="section-title">Dados Brutos</div>', unsafe_allow_html=True)
        st.dataframe(df_cohort, use_container_width=True)