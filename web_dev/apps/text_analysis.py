import pandas as pd
import plotly.express as px
#import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

from app import app

layout = html.Div([
    html.H2('text_analysis', id='header', style={'text-align': 'center'}),
])