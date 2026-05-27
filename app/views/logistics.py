# src/streamlit/pages/logistics.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from components.database import query
from components.theme import CARD_CSS, PLOTLY_THEME


def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    st.markdown("""
    <div class="dash-header">
        <h1>Logística</h1>
        <p>Performance de entrega, SLA e atrasos por região</p>
    </div>
    """, unsafe_allow_html=True)

    # ── KPIs ─────────────────────────────────────────────────
    kpis = query("""
        SELECT 
            AVG(avg_delivery_days) as avg_delivery,
            AVG(delay_rate_pct) as avg_delay,
            AVG(sla_compliance_pct) as avg_sla,
            SUM(total_delayed) as total_delayed,
            SUM(total_delivered) as total_delivered
        FROM metrics_delivery
    """)

    avg_del   = kpis['avg_delivery'].iloc[0]
    avg_delay = kpis['avg_delay'].iloc[0]
    avg_sla   = kpis['avg_sla'].iloc[0]
    delayed   = int(kpis['total_delayed'].iloc[0])
    delivered = int(kpis['total_delivered'].iloc[0])

    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card blue">
            <div class="kpi-label">Tempo Médio Entrega</div>
            <div class="kpi-value">{avg_del:.1f}d</div>
            <div class="kpi-sub">dias em média</div>
        </div>
        <div class="kpi-card red">
            <div class="kpi-label">Taxa de Atraso</div>
            <div class="kpi-value">{avg_delay:.1f}%</div>
            <div class="kpi-sub">dos pedidos</div>
        </div>
        <div class="kpi-card green">
            <div class="kpi-label">SLA Compliance</div>
            <div class="kpi-value">{avg_sla:.1f}%</div>
            <div class="kpi-sub">dentro do prazo</div>
        </div>
        <div class="kpi-card yellow">
            <div class="kpi-label">Pedidos Atrasados</div>
            <div class="kpi-value">{delayed:,}</div>
            <div class="kpi-sub">de {delivered:,} entregues</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Evolução temporal ────────────────────────────────────
    st.markdown('<div class="section-title">Evolução Temporal</div>', unsafe_allow_html=True)

    df = query("""
        SELECT year, month, avg_delivery_days, avg_estimated_days
        FROM metrics_delivery ORDER BY year, month
    """)
    df["periodo"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

    df_delay = query("""
        SELECT year, month, delay_rate_pct
        FROM metrics_delivery ORDER BY year, month
    """)
    df_delay["periodo"] = df_delay["year"].astype(str) + "-" + df_delay["month"].astype(str).str.zfill(2)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Tempo Médio de Entrega por Mês</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["periodo"], y=df["avg_delivery_days"],
            name="Real", mode='lines+markers',
            line=dict(color='#4f46e5', width=2),
            marker=dict(size=4)
        ))
        fig.add_trace(go.Scatter(
            x=df["periodo"], y=df["avg_estimated_days"],
            name="Estimado", mode='lines',
            line=dict(color='#f87171', width=2, dash='dash')
        ))
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Taxa de Atraso por Mês</div>', unsafe_allow_html=True)
        fig = px.bar(df_delay, x="periodo", y="delay_rate_pct",
                     labels={"delay_rate_pct": "Taxa (%)", "periodo": ""},
                     color_discrete_sequence=["#dc2626"])
        fig.update_traces(marker_line_width=0, opacity=0.85)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Por estado ───────────────────────────────────────────
    st.markdown('<div class="section-title">Performance por Estado</div>', unsafe_allow_html=True)

    df_state = query("""
        SELECT customer_state, avg_delivery_days, delay_rate_pct, total_delivered
        FROM metrics_delivery_by_state ORDER BY avg_delivery_days DESC
    """)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Top 10 Estados — Maior Tempo de Entrega</div>', unsafe_allow_html=True)
        fig = px.bar(df_state.head(10), x="avg_delivery_days", y="customer_state",
                     orientation="h",
                     labels={"avg_delivery_days": "Dias", "customer_state": ""},
                     color="avg_delivery_days", color_continuous_scale="Blues")
        fig.update_traces(marker_line_width=0)
        fig.update_layout(**PLOTLY_THEME, height=300,
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Top 10 Estados — Maior Taxa de Atraso</div>', unsafe_allow_html=True)
        df_sorted = df_state.sort_values("delay_rate_pct", ascending=False)
        fig = px.bar(df_sorted.head(10), x="delay_rate_pct", y="customer_state",
                     orientation="h",
                     labels={"delay_rate_pct": "Taxa (%)", "customer_state": ""},
                     color="delay_rate_pct", color_continuous_scale="Reds")
        fig.update_traces(marker_line_width=0)
        fig.update_layout(**PLOTLY_THEME, height=300,
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    if True:
        st.markdown('<div class="section-title">Dados Brutos</div>', unsafe_allow_html=True)
        st.dataframe(df_state, use_container_width=True)