# src/streamlit/pages/technical.py
import streamlit as st
import plotly.express as px
from components.database import query
from components.theme import CARD_CSS, PLOTLY_THEME


def render():
    st.markdown(CARD_CSS, unsafe_allow_html=True)

    st.markdown("""
    <div class="dash-header">
        <h1>🔧 Dados Técnicos</h1>
        <p>Qualidade dos dados, flags de inconsistência e query livre</p>
        <span class="period-badge">🛠 Acesso desenvolvedor</span>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs([
        "Qualidade", "Flags", "Schemas", "Query Livre"
    ])

    # ── Tab 1: Qualidade ─────────────────────────────────────
    with tab1:
        st.markdown('<div class="section-title">Contagem de Registros</div>', unsafe_allow_html=True)

        tables = [
            ("fact_orders",      "", "blue"),
            ("fact_order_items", "", "green"),
            ("dim_customers",    "", "purple"),
            ("dim_products",     "", "yellow"),
            ("dim_sellers",      "", "cyan"),
        ]

        counts = {}
        for table, _, _ in tables:
            df = query(f"SELECT COUNT(*) as total FROM {table}")
            counts[table] = int(df["total"].iloc[0])

        cards_html = '<div class="kpi-grid">'
        for table, icon, color in tables:
            cards_html += f"""
            <div class="kpi-card {color}">
                <span class="kpi-icon">{icon}</span>
                <div class="kpi-label">{table}</div>
                <div class="kpi-value sm">{counts[table]:,}</div>
                <div class="kpi-sub">registros</div>
            </div>"""
        cards_html += '</div>'
        st.markdown(cards_html, unsafe_allow_html=True)

    # ── Tab 2: Flags ─────────────────────────────────────────
    with tab2:
        st.markdown('<div class="section-title">Inconsistências em fact_orders</div>', unsafe_allow_html=True)

        flags = query("""
            SELECT
                SUM(flag_financial_inconsistency::int) as financial_inconsistency,
                SUM(flag_delayed::int) as delayed,
                SUM(flag_delivered_before_purchase::int) as delivered_before_purchase,
                SUM(flag_estimated_before_purchase::int) as estimated_before_purchase,
                SUM(flag_future_purchase::int) as future_purchase,
                COUNT(*) as total
            FROM fact_orders
        """)

        fi   = int(flags['financial_inconsistency'].iloc[0])
        del_ = int(flags['delayed'].iloc[0])
        dbp  = int(flags['delivered_before_purchase'].iloc[0])
        ebp  = int(flags['estimated_before_purchase'].iloc[0])
        fp   = int(flags['future_purchase'].iloc[0])
        tot  = int(flags['total'].iloc[0])

        st.markdown(f"""
        <div class="kpi-grid">
            <div class="kpi-card red">
                <div class="kpi-label">Inconsistência Financeira</div>
                <div class="kpi-value sm">{fi:,}</div>
                <div class="kpi-sub">{fi/tot*100:.2f}% do total</div>
            </div>
            <div class="kpi-card yellow">
                <div class="kpi-label">Atrasados</div>
                <div class="kpi-value sm">{del_:,}</div>
                <div class="kpi-sub">{del_/tot*100:.1f}% do total</div>
            </div>
            <div class="kpi-card purple">
                <div class="kpi-label">Entregue antes da compra</div>
                <div class="kpi-value sm">{dbp:,}</div>
                <div class="kpi-sub">{dbp/tot*100:.2f}% do total</div>
            </div>
            <div class="kpi-card cyan">
                <div class="kpi-label">Data futura inválida</div>
                <div class="kpi-value sm">{fp:,}</div>
                <div class="kpi-sub">{fp/tot*100:.2f}% do total</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Gráfico de barras das flags
        st.markdown('<div class="chart-card"><div class="chart-title">Volume de Flags</div>', unsafe_allow_html=True)
        import pandas as pd
        df_flags = pd.DataFrame({
            "Flag": ["Financeira", "Atraso", "Entrega < Compra", "Estimativa < Compra", "Data Futura"],
            "Total": [fi, del_, dbp, ebp, fp]
        })
        fig = px.bar(df_flags, x="Flag", y="Total",
                     color="Total", color_continuous_scale="Reds",
                     labels={"Total": "Ocorrências"})
        fig.update_traces(marker_line_width=0)
        fig.update_layout(**PLOTLY_THEME, height=260, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Tab 3: Schemas ───────────────────────────────────────
    with tab3:
        st.markdown('<div class="section-title">Schema das Tabelas</div>', unsafe_allow_html=True)
        table = st.selectbox("Selecione a tabela", [
            "fact_orders", "fact_order_items", "dim_customers",
            "dim_products", "dim_sellers", "dim_dates",
            "metrics_revenue", "metrics_delivery", "metrics_abc"
        ])
        df = query(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """)
        st.dataframe(df, use_container_width=True)

    # ── Tab 4: Query Livre ───────────────────────────────────
    with tab4:
        st.markdown('<div class="section-title">Query Livre</div>', unsafe_allow_html=True)
        st.warning("Apenas queries SELECT são permitidas.")
        sql = st.text_area("SQL", value="SELECT * FROM fact_orders LIMIT 10", height=150)
        if st.button("▶️ Executar", type="primary"):
            if sql.strip().upper().startswith("SELECT"):
                try:
                    df = query(sql)
                    st.success(f"{len(df)} rows retornados")
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(f"Erro: {e}")
            else:
                st.error("Apenas queries SELECT são permitidas.")