import sys
import os

sys.path.append(os.path.dirname(__file__))

import streamlit as st
from components.sidebar import render_sidebar

st.set_page_config(
    page_title="TCC Data Engineering",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

page = render_sidebar()

if page == "overview":
    from views.overview import render
elif page == "revenue":
    from views.revenue import render
elif page == "logistics":
    from views.logistics import render
elif page == "reviews":
    from views.reviews import render
elif page == "customers":
    from views.customers import render
elif page == "ranking":
    from views.ranking import render
elif page == "technical":
    from views.technical import render

render()