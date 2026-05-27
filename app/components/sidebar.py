import streamlit as st


def render_sidebar() -> str:

    with st.sidebar:
        st.markdown("## 📊 TCC Data Engineering")
        st.markdown("### Olist E-Commerce Analytics")
        st.divider()

        st.markdown("### Navegação")

        pages = {
            "📊 Overview": "overview",
            "💰 Receita": "revenue",
            "🚚 Logística": "logistics",
            "⭐ Reviews / NPS": "reviews",
            "👥 Clientes": "customers",
            "🏆 Rankings": "ranking",
            "🔧 Dados Técnicos": "technical",
        }

        selected = st.radio(
            label="",
            options=list(pages.keys()),
            label_visibility="collapsed"
        )

    return pages[selected]