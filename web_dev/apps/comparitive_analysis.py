import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob
from iso3166 import countries

from app_temp import app

colorscale = ["#deebf7", "#d2e3f3", "#c6dbef", "#b3d2e9", "#9ecae1",
    "#85bcdb", "#6baed6", "#57a0ce", "#4292c6", "#3082be", "#2171b5", "#1361a9",
    "#08519c", "#0b4083", "#08306b"
]

path_list = ['./apps/analysis_data/task5', './apps/analysis_data/task9', './apps/analysis_data/task14'] # use your path
task_list = ['task5', 'task9', 'task14']
df = {}

for i, path in enumerate(path_list):
    filenames = glob.glob(path+"/*.parquet")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_parquet(filename))
    # Concatenate all data into one DataFrame
    df[task_list[i]] = pd.concat(dfs, ignore_index=True)

df['task14'] = df['task14'].sort_values(by=['month'])

#******task5******
task5_fig = px.scatter(df['task5'], x='month', y='revenue',
	    size="count", color="genre",
        hover_name="genre", log_x=True,
        text="genre",
        size_max=60, template="plotly_white",
    )
task5_fig.update_layout(
        #title="Plot Title",
        xaxis_title='Month',
        yaxis_title='Revenue',
)

#******task9******
task9_fig = px.scatter(df['task9'], x='budget', y='revenue',
    size="count", color="genre",
    hover_name="genre", log_x=True,
    text="genre",
    size_max=60, template="plotly_white",
)
task9_fig.update_layout(
    #title="Plot Title",
    xaxis_title='Budget',
    yaxis_title='Revenue',
)

#******task14******
task14_pie_fig = px.pie(df['task14'], values='count', names='month', title='Number of Movies Released Each Month')
task14_bar_fig = px.bar(data_frame=df['task14'], y='avg_profit', x='month', orientation='v', text='month', template="plotly_white", title='Average Profit Each Month')
task14_bar_fig.update_layout(
    xaxis_title="Month",
    yaxis_title='Average Profit',
)
layout = html.Div([
    html.Div(id='task5_container', children=[
        html.H2(id='header_task5', style={'text-align': 'center'}, children='Average Revenue Per Release Month for Movies in Each Genres'),
        html.Div(id='task5_p', children=[
            html.P(
                id="task5_insight",
                children="† Deaths are classified using the International Classification of Diseases, \
                Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                (undetermined intent).***count of language is the size of bubble here.***",
            ),
        ]),
        dcc.Graph(id='task5_bubble_chart', figure=task5_fig)
    ]),

    html.Div(id='task9_container', children=[
        html.H2(id='header_task9', style={'text-align': 'center'}, children='Average Budget and Average Revenue Comparison for Movies in Each Genres'),
        html.Div(id='task9_p', children=[
            html.P(
                id="task9_insight",
                children="† Deaths are classified using the International Classification of Diseases, \
                Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                (undetermined intent).***count of language is the size of bubble here.***",
            ),
        ]),
        dcc.Graph(id='task9_bubble_chart', figure=task9_fig)
    ]),

    html.Div(id='task14_container', children=[
        html.H2(id='header_task14', style={'text-align': 'center'}, children='Average Budget and Average Revenue Comparison for Movies in Each Genres'),
        html.Div(id='task14_p', children=[
            html.P(
                id="task14_insight",
                children="† Deaths are classified using the International Classification of Diseases, \
                Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                (undetermined intent).***count of language is the size of bubble here.***",
            ),
        ]),
        dcc.Graph(id='task14_pie_chart', figure=task14_pie_fig),
        dcc.Graph(id='task14_bar_chart', figure=task14_bar_fig)
    ]),

])
