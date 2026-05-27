# src/streamlit/pages/reviews.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from components.database import query
from components.theme import CARD_CSS, PLOTLY_THEME


def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    st.markdown("""
    <div class="dash-header">
        <h1>Reviews / NPS</h1>
        <p>Satisfação dos clientes, NPS e avaliações por categoria</p>
    </div>
    """, unsafe_allow_html=True)

    # ── KPIs ─────────────────────────────────────────────────
    kpis = query("""
        SELECT 
            AVG(nps) as avg_nps,
            SUM(avg_score * total_reviews) / SUM(total_reviews) as avg_score,
            SUM(total_reviews) as total_reviews,
            SUM(promoters) as promoters,
            SUM(detractors) as detractors
        FROM metrics_reviews
    """)

    avg_nps    = kpis['avg_nps'].iloc[0]
    avg_score  = kpis['avg_score'].iloc[0]
    total_rev  = int(kpis['total_reviews'].iloc[0])
    promoters  = int(kpis['promoters'].iloc[0])
    detractors = int(kpis['detractors'].iloc[0])

    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card blue">
            <div class="kpi-label">NPS Médio</div>
            <div class="kpi-value">{avg_nps:.1f}</div>
            <div class="kpi-sub">Net Promoter Score</div>
        </div>
        <div class="kpi-card yellow">
            <div class="kpi-label">Score Médio</div>
            <div class="kpi-value">{avg_score:.2f}<span style="font-size:1rem;color:#6b7280">/5</span></div>
            <div class="kpi-sub">ponderado por volume</div>
        </div>
        <div class="kpi-card green">
            <div class="kpi-label">Total Reviews</div>
            <div class="kpi-value">{total_rev:,}</div>
            <div class="kpi-sub">avaliações</div>
        </div>
        <div class="kpi-card purple">
            <div class="kpi-label">Promotores / Detratores</div>
            <div class="kpi-value sm">{promoters:,} / {detractors:,}</div>
            <div class="kpi-sub">scores 4-5 vs 1-2</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── NPS + Distribuição ───────────────────────────────────
    st.markdown('<div class="section-title">Análise NPS</div>', unsafe_allow_html=True)

    df = query("""
        SELECT year, month, nps, avg_score, total_reviews
        FROM metrics_reviews ORDER BY year, month
    """)
    df["periodo"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

    df_dist = query("""
        SELECT SUM(promoters) as promotores, SUM(neutrals) as neutros, SUM(detractors) as detratores
        FROM metrics_reviews
    """)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">NPS Mensal</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["periodo"], y=df["nps"],
            marker_color=df["nps"].apply(lambda x: "#34d399" if x >= 0 else "#f87171"),
            marker_line_width=0, opacity=0.9
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="#4b5563", line_width=1)
        fig.update_layout(**PLOTLY_THEME, height=260)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Distribuição NPS</div>', unsafe_allow_html=True)
        fig = go.Figure(go.Pie(
            values=[df_dist["promotores"].iloc[0], df_dist["neutros"].iloc[0], df_dist["detratores"].iloc[0]],
            labels=["Promotores (4-5)", "Neutros (3)", "Detratores (1-2)"],
            hole=0.55,
            marker=dict(colors=["#34d399", "#fbbf24", "#f87171"],
                        line=dict(color='#0a0a0f', width=3)),
            textfont=dict(color='#e5e7eb')
        ))
        fig.update_layout(**PLOTLY_THEME, height=260, showlegend=True)
        fig.update_layout(legend=dict(orientation="h", y=-0.15, font=dict(color='#9ca3af')))
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Score por categoria ──────────────────────────────────
    st.markdown('<div class="section-title">Por Categoria</div>', unsafe_allow_html=True)

    df_cat = query("""
        SELECT product_category_name_english, avg_score, total_reviews
        FROM metrics_reviews_by_category
        WHERE product_category_name_english IS NOT NULL
        ORDER BY avg_score DESC LIMIT 20
    """)

    st.markdown('<div class="chart-card"><div class="chart-title">Score Médio por Categoria (Top 20)</div>', unsafe_allow_html=True)
    fig = px.bar(df_cat, x="avg_score", y="product_category_name_english",
                 orientation="h", color="avg_score",
                 color_continuous_scale=["#f87171", "#fbbf24", "#34d399"],
                 range_color=[1, 5],
                 labels={"avg_score": "Score Médio", "product_category_name_english": ""})
    fig.update_traces(marker_line_width=0)
    fig.update_layout(**PLOTLY_THEME, height=500,
                      coloraxis_colorbar=dict(tickfont=dict(color='#9ca3af')))
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if True:
        st.markdown('<div class="section-title">Dados Brutos</div>', unsafe_allow_html=True)
        df_raw = query("SELECT * FROM metrics_reviews ORDER BY year, month")
        st.dataframe(df_raw, use_container_width=True)