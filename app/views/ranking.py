# src/streamlit/pages/rankings.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from components.database import query
from components.theme import CARD_CSS, PLOTLY_THEME, fmt_brl


def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    st.markdown("""
    <div class="dash-header">
        <h1>Rankings</h1>
        <p>Top sellers, categorias e curva ABC de produtos</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Top Sellers + Categorias ─────────────────────────────
    st.markdown('<div class="section-title">Top Performers</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title">Top 10 Sellers por Receita</div>', unsafe_allow_html=True)
        df_sellers = query("""
            SELECT s.seller_id, s.seller_city, s.seller_state,
                   r.total_revenue, r.total_orders
            FROM metrics_revenue_by_seller r
            JOIN dim_sellers s ON r.seller_id = s.seller_id
            ORDER BY r.total_revenue DESC LIMIT 10
        """)
        # Abreviar seller_id para display
        df_sellers["seller_short"] = df_sellers["seller_id"].str[:8] + "..."
        fig = px.bar(df_sellers, x="total_revenue", y="seller_short",
                     orientation="h",
                     hover_data=["seller_city", "seller_state", "total_orders"],
                     labels={"total_revenue": "Receita (R$)", "seller_short": ""},
                     color="total_revenue", color_continuous_scale="Blues")
        fig.update_traces(marker_line_width=0)
        fig.update_layout(**PLOTLY_THEME, height=320, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Top 10 Categorias por Volume</div>', unsafe_allow_html=True)
        df_cat = query("""
            SELECT product_category_name_english, total_orders, total_revenue
            FROM metrics_revenue_by_category
            ORDER BY total_orders DESC LIMIT 10
        """)
        fig = px.bar(df_cat, x="total_orders", y="product_category_name_english",
                     orientation="h",
                     labels={"total_orders": "Pedidos", "product_category_name_english": ""},
                     color="total_orders", color_continuous_scale="Purples")
        fig.update_traces(marker_line_width=0)
        fig.update_layout(**PLOTLY_THEME, height=320, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Curva ABC ────────────────────────────────────────────
    st.markdown('<div class="section-title">Curva ABC</div>', unsafe_allow_html=True)

    df_abc = query("""
        SELECT abc_class, COUNT(*) as total_products, SUM(total_revenue) as total_revenue
        FROM metrics_abc GROUP BY abc_class ORDER BY abc_class
    """)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card"><div class="chart-title"> Receita por Classe ABC</div>', unsafe_allow_html=True)
        fig = go.Figure(go.Pie(
            values=df_abc["total_revenue"],
            labels=df_abc["abc_class"],
            hole=0.55,
            marker=dict(
                colors=["#4f46e5", "#fbbf24", "#f87171"],
                line=dict(color='#0a0a0f', width=3)
            ),
            textfont=dict(color='#e5e7eb')
        ))
        fig.update_layout(**PLOTLY_THEME, height=280, showlegend=True,
                          legend=dict(orientation="h", y=-0.1, font=dict(color='#9ca3af')))
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card"><div class="chart-title">Produtos por Classe</div>', unsafe_allow_html=True)
        color_map = {"A": "#4f46e5", "B": "#fbbf24", "C": "#f87171"}
        fig = px.bar(df_abc, x="abc_class", y="total_products",
                     labels={"total_products": "Produtos", "abc_class": "Classe"},
                     color="abc_class", color_discrete_map=color_map)
        fig.update_traces(marker_line_width=0, opacity=0.9)
        fig.update_layout(**PLOTLY_THEME, height=280, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Tabela ABC ───────────────────────────────────────────
    st.markdown('<div class="section-title">Detalhamento ABC</div>', unsafe_allow_html=True)

    df_detail = query("""
        SELECT abc_class, product_id, product_category_name_english,
               total_revenue, total_orders,
               ROUND(revenue_pct::numeric, 2) as revenue_pct,
               ROUND(cumulative_revenue_pct::numeric, 2) as cumulative_revenue_pct
        FROM metrics_abc ORDER BY abc_class, total_revenue DESC
    """)

    tab_a, tab_b, tab_c = st.tabs(["🔵 Classe A", "🟡 Classe B", "🔴 Classe C"])
    with tab_a:
        st.dataframe(df_detail[df_detail["abc_class"] == "A"], use_container_width=True)
    with tab_b:
        st.dataframe(df_detail[df_detail["abc_class"] == "B"], use_container_width=True)
    with tab_c:
        st.dataframe(df_detail[df_detail["abc_class"] == "C"], use_container_width=True)

    if True:
        st.markdown('<div class="section-title">Dados Brutos — Sellers</div>', unsafe_allow_html=True)
        df_raw = query("SELECT * FROM metrics_revenue_by_seller ORDER BY total_revenue DESC")
        st.dataframe(df_raw, use_container_width=True)