# components/theme.py
# Tema compartilhado entre todas as views

CARD_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600;700&display=swap');

[data-testid="stAppViewContainer"] { background: #0a0a0f; }
[data-testid="stSidebar"] { background: #0f0f18 !important; }
[data-testid="stHeader"] { background: transparent; }
[data-testid="stMetricLabel"] { display: none; }
[data-testid="stMetricValue"] { display: none; }
[data-testid="stMetricDelta"] { display: none; }

.block-container { padding-top: 2rem !important; padding-bottom: 2rem !important; }

.dash-header { margin-bottom: 2rem; }
.dash-header h1 {
    font-family: 'Space Mono', monospace;
    font-size: 2.2rem; font-weight: 700;
    color: #ffffff; letter-spacing: -0.02em; margin: 0 0 0.25rem 0;
}
.dash-header p { font-family: 'DM Sans', sans-serif; font-size: 0.95rem; color: #6b7280; margin: 0; }
.dash-header .period-badge {
    display: inline-block; background: #1a1a2e;
    border: 1px solid #2d2d4e; border-radius: 6px;
    padding: 4px 12px; font-family: 'Space Mono', monospace;
    font-size: 0.75rem; color: #8b8bff; margin-top: 0.5rem;
}

.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1.5rem; }
.kpi-grid-5 { display: grid; grid-template-columns: repeat(5, 1fr); gap: 1rem; margin-bottom: 1.5rem; }

.kpi-card {
    background: linear-gradient(135deg, #13131f 0%, #1a1a2e 100%);
    border: 1px solid #2d2d4e; border-radius: 14px;
    padding: 1.4rem 1.5rem; position: relative; overflow: hidden;
    transition: transform 0.2s, border-color 0.2s;
}
.kpi-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0;
    height: 3px; border-radius: 14px 14px 0 0;
}
.kpi-card.blue::before   { background: linear-gradient(90deg, #4f46e5, #818cf8); }
.kpi-card.green::before  { background: linear-gradient(90deg, #059669, #34d399); }
.kpi-card.yellow::before { background: linear-gradient(90deg, #d97706, #fbbf24); }
.kpi-card.red::before    { background: linear-gradient(90deg, #dc2626, #f87171); }
.kpi-card.purple::before { background: linear-gradient(90deg, #7c3aed, #a78bfa); }
.kpi-card.cyan::before   { background: linear-gradient(90deg, #0891b2, #22d3ee); }
.kpi-card:hover { transform: translateY(-2px); border-color: #4d4d7f; }

.kpi-label {
    font-family: 'DM Sans', sans-serif; font-size: 0.78rem; font-weight: 500;
    color: #6b7280; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.6rem;
}
.kpi-value {
    font-family: 'Space Mono', monospace; font-size: 1.7rem; font-weight: 700;
    color: #f9fafb; line-height: 1.1; margin-bottom: 0.4rem;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.kpi-value.sm { font-size: 1.3rem; }
.kpi-sub { font-family: 'DM Sans', sans-serif; font-size: 0.78rem; color: #9ca3af; }
.kpi-icon { position: absolute; top: 1.2rem; right: 1.2rem; font-size: 1.5rem; opacity: 0.5; }

.section-title {
    font-family: 'Space Mono', monospace; font-size: 0.85rem; font-weight: 700;
    color: #8b8bff; text-transform: uppercase; letter-spacing: 0.1em;
    margin: 1.5rem 0 0.75rem 0; display: flex; align-items: center; gap: 8px;
}
.section-title::after { content: ''; flex: 1; height: 1px; background: linear-gradient(90deg, #2d2d4e, transparent); }

.chart-card {
    background: #13131f; border: 1px solid #2d2d4e;
    border-radius: 14px; padding: 1.25rem 1.25rem 0.5rem 1.25rem; margin-bottom: 1rem;
}
.chart-title { font-family: 'DM Sans', sans-serif; font-size: 0.88rem; font-weight: 600; color: #e5e7eb; margin-bottom: 0.75rem; }

hr { border-color: #1f1f35 !important; }

/* Tabs */
[data-testid="stTabs"] button {
    font-family: 'DM Sans', sans-serif !important;
    color: #6b7280 !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #8b8bff !important;
    border-bottom-color: #8b8bff !important;
}

/* Dataframe */
[data-testid="stDataFrame"] { border: 1px solid #2d2d4e !important; border-radius: 10px; }

/* Selectbox / inputs */
[data-testid="stSelectbox"] label { font-family: 'DM Sans', sans-serif !important; color: #9ca3af !important; font-size: 0.8rem !important; }

/* Warning / success */
[data-testid="stAlert"] { border-radius: 10px !important; }
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

def inject_css(st):
    st.markdown(CARD_CSS, unsafe_allow_html=True)