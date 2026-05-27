# src/streamlit/pages/overview.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from components.database import query

CARD_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600;700&display=swap');

/* Reset global */
[data-testid="stAppViewContainer"] {
    background: #0a0a0f;
}
[data-testid="stSidebar"] {
    background: #0f0f18 !important;
}
[data-testid="stHeader"] {
    background: transparent;
}
.block-container {
    padding-top: 2rem !important;
    padding-bottom: 2rem !important;
}

/* Esconde elementos padrão do Streamlit */
[data-testid="stMetricLabel"] { display: none; }
[data-testid="stMetricValue"] { display: none; }
[data-testid="stMetricDelta"] { display: none; }

/* Header da página */
.dash-header {
    margin-bottom: 2rem;
}
.dash-header h1 {
    font-family: 'Space Mono', monospace;
    font-size: 2.2rem;
    font-weight: 700;
    color: #ffffff;
    letter-spacing: -0.02em;
    margin: 0 0 0.25rem 0;
}
.dash-header p {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.95rem;
    color: #6b7280;
    margin: 0;
}
.dash-header .period-badge {
    display: inline-block;
    background: #1a1a2e;
    border: 1px solid #2d2d4e;
    border-radius: 6px;
    padding: 4px 12px;
    font-family: 'Space Mono', monospace;
    font-size: 0.75rem;
    color: #8b8bff;
    margin-top: 0.5rem;
}

/* KPI Cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 1.5rem;
}
.kpi-card {
    background: linear-gradient(135deg, #13131f 0%, #1a1a2e 100%);
    border: 1px solid #2d2d4e;
    border-radius: 14px;
    padding: 1.4rem 1.5rem;
    position: relative;
    overflow: hidden;
    transition: transform 0.2s, border-color 0.2s;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    border-radius: 14px 14px 0 0;
}
.kpi-card.blue::before  { background: linear-gradient(90deg, #4f46e5, #818cf8); }
.kpi-card.green::before { background: linear-gradient(90deg, #059669, #34d399); }
.kpi-card.yellow::before{ background: linear-gradient(90deg, #d97706, #fbbf24); }
.kpi-card.red::before   { background: linear-gradient(90deg, #dc2626, #f87171); }

.kpi-card:hover {
    transform: translateY(-2px);
    border-color: #4d4d7f;
}
.kpi-label {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.78rem;
    font-weight: 500;
    color: #6b7280;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.6rem;
}
.kpi-value {
    font-family: 'Space Mono', monospace;
    font-size: 1.7rem;
    font-weight: 700;
    color: #f9fafb;
    line-height: 1.1;
    margin-bottom: 0.4rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.kpi-sub {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.78rem;
    color: #9ca3af;
}
.kpi-icon {
    position: absolute;
    top: 1.2rem;
    right: 1.2rem;
    font-size: 1.5rem;
    opacity: 0.5;
}

/* Section titles */
.section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.85rem;
    font-weight: 700;
    color: #8b8bff;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin: 1.5rem 0 0.75rem 0;
    display: flex;
    align-items: center;
    gap: 8px;
}
.section-title::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, #2d2d4e, transparent);
}

/* Chart containers */
.chart-card {
    background: #13131f;
    border: 1px solid #2d2d4e;
    border-radius: 14px;
    padding: 1.25rem 1.25rem 0.5rem 1.25rem;
    margin-bottom: 1rem;
}
.chart-title {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.88rem;
    font-weight: 600;
    color: #e5e7eb;
    margin-bottom: 0.75rem;
}

/* Divider */
hr { border-color: #1f1f35 !important; }
</style>
"""

PLOTLY_THEME = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='DM Sans', color='#9ca3af', size=11),
    xaxis=dict(gridcolor='#1f1f35', linecolor='#2d2d4e', tickcolor='#4b5563'),
    yaxis=dict(gridcolor='#1f1f35', linecolor='#2d2d4e', tickcolor='#4b5563'),
    margin=dict(l=0, r=0, t=10, b=0),
)


def fmt_brl(value):
    if value >= 1_000_000:
        return f"R$ {value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"R$ {value/1_000:.0f}k"
    return f"R$ {value:,.0f}"


def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    # ── Header ──────────────────────────────────────────────
    st.markdown("""
    <div class="dash-header">
        <h1>Overview</h1>
        <p>Visão geral do desempenho do negócio Olist</p>
        <span class="period-badge">⏱ Set 2016 — Set 2018</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Queries ─────────────────────────────────────────────
    revenue   = query("SELECT SUM(total_revenue) as total FROM metrics_revenue")
    orders    = query("SELECT SUM(total_orders) as total FROM metrics_revenue")
    avg_tick  = query("SELECT SUM(total_revenue)/SUM(total_orders) as avg FROM metrics_revenue")
    delay     = query("SELECT AVG(delay_rate_pct) as avg FROM metrics_delivery")

    total_rev    = revenue["total"].iloc[0]
    total_orders = int(orders["total"].iloc[0])
    ticket       = avg_tick["avg"].iloc[0]
    delay_rate   = delay["avg"].iloc[0]

    # ── KPI Cards ───────────────────────────────────────────
    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card blue">
            <div class="kpi-label">Receita Total</div>
            <div class="kpi-value">{fmt_brl(total_rev)}</div>
            <div class="kpi-sub">Set/2016 – Set/2018</div>
        </div>
        <div class="kpi-card green">
            <div class="kpi-label">Total de Pedidos</div>
            <div class="kpi-value">{total_orders:,}</div>
            <div class="kpi-sub">pedidos entregues</div>
        </div>
        <div class="kpi-card yellow">
            <div class="kpi-label">Ticket Médio</div>
            <div class="kpi-value">{fmt_brl(ticket)}</div>
            <div class="kpi-sub">por pedido</div>
        </div>
        <div class="kpi-card red">
            <div class="kpi-label">Taxa de Atraso</div>
            <div class="kpi-value">{delay_rate:.1f}%</div>
            <div class="kpi-sub">dos pedidos atrasaram</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Receita Mensal + Estado ──────────────────────────────
    st.markdown('<div class="section-title">Receita</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Receita por Mês</div>', unsafe_allow_html=True)
        df_rev = query("""
            SELECT year, month, total_revenue
            FROM metrics_revenue ORDER BY year, month
        """)
        df_rev["periodo"] = df_rev["year"].astype(str) + "-" + df_rev["month"].astype(str).str.zfill(2)
        fig = px.bar(df_rev, x="periodo", y="total_revenue",
                     labels={"total_revenue": "Receita (R$)", "periodo": ""},
                     color_discrete_sequence=["#4f46e5"])
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Top 10 Estados por Receita</div>', unsafe_allow_html=True)
        df_state = query("""
            SELECT customer_state, total_revenue FROM metrics_revenue_by_state
            ORDER BY total_revenue DESC LIMIT 10
        """)
        fig = px.bar(df_state, x="total_revenue", y="customer_state", orientation="h",
                     labels={"total_revenue": "Receita (R$)", "customer_state": ""},
                     color_discrete_sequence=["#059669"])
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Status + NPS ─────────────────────────────────────────
    st.markdown('<div class="section-title">Operacional</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Status dos Pedidos</div>', unsafe_allow_html=True)
        df_status = query("""
            SELECT order_status, COUNT(*) as total
            FROM fact_orders GROUP BY order_status ORDER BY total DESC
        """)
        fig = px.pie(df_status, values="total", names="order_status",
                     color_discrete_sequence=["#4f46e5","#059669","#d97706","#dc2626","#7c3aed","#0891b2"])
        fig.update_traces(textposition='inside', textinfo='percent+label',
                          marker=dict(line=dict(color='#0a0a0f', width=2)))
        fig.update_layout(**PLOTLY_THEME, height=260, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">NPS ao Longo do Tempo</div>', unsafe_allow_html=True)
        df_nps = query("""
            SELECT year, month, nps FROM metrics_reviews ORDER BY year, month
        """)
        df_nps["periodo"] = df_nps["year"].astype(str) + "-" + df_nps["month"].astype(str).str.zfill(2)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_nps["periodo"], y=df_nps["nps"],
            mode='lines+markers',
            line=dict(color='#fbbf24', width=2),
            marker=dict(size=5, color='#fbbf24'),
            fill='tozeroy',
            fillcolor='rgba(251,191,36,0.08)'
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="#4b5563", line_width=1)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)