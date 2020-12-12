import pandas as pd
import plotly.express as px
#import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

from app_temp import app
colorscale = [ "#c6dbef", "#b3d2e9", "#9ecae1",
    "#85bcdb", "#6baed6", "#57a0ce", "#4292c6", "#3082be", "#2171b5", "#1361a9",
    "#08519c", "#0b4083", "#08306b"
]
# ------------------------------------------------------------------------------
# Import and clean data (importing csv into pandas)
path_list = ['./apps/analysis_data/task1', './apps/analysis_data/task2', './apps/analysis_data/task3', './apps/analysis_data/task4', './apps/analysis_data/task16'] # use your path
task_list = ['task1', 'task2', 'task3', 'task4', 'task16']
df = {}
for i, path in enumerate(path_list):
    filenames = glob.glob(path+"/*.parquet")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_parquet(filename))
    # Concatenate all data into one DataFrame
    df[task_list[i]] = pd.concat(dfs, ignore_index=True)

genre_list = []
for genre_name in df['task3']['genre_name'].unique():
    genre_list.append({"label": genre_name, "value": genre_name})

year_label_list = []
year_list = []
for i in range(18):
    year_label_list.append({"label": 2000+(17-i), "value": 2000+(17-i)})
    year_list.append(2000 + i)

col_label = {}
col_label['vote_average'] = 'TMDB critics rating (Max=10)'
col_label['avg_user_rating'] = 'Average User Rating (Max=5)'
col_label['popularity'] = 'Popularity'
col_label['profit'] = 'Profit'

col_list = [{"label": col_label["popularity"], "value": "popularity"},
            {"label": col_label["profit"], "value": "profit"},
            {"label": col_label['vote_average'], "value": "vote_average"},
            {"label": col_label['avg_user_rating'], "value": "avg_user_rating"}]

# task16_col_label = {}
# task16_col_label['vote_average'] = 'TMDB critics rating (Max=10)'
# task16_col_label['avg_user_rating'] = 'Average User Rating (Max=5)'

job_list = []
for job_name in df['task16']['job'].unique():
    job_list.append({"label": job_name, "value": job_name})
# ------------------------------------------------------------------------------
# App layout
layout = html.Div([
    html.Div(id='task1_container', children=[
        html.H2(id='header_task1', style={'text-align': 'center'}),
        html.Div(id='task1_sub', children=[
            html.Div(id='task1_choice', children=[
                html.Label('Year:'),
                dcc.Dropdown(id="slct_year_task1",
                            options=year_label_list,
                            multi=False,
                            value=2017,
                            clearable=False,
                            ),
                html.Label('Parameter:'),
                dcc.Dropdown(id="slct_col_task1",
                            options=col_list,
                            multi=False,
                            value="popularity",
                            clearable=False,
                            ),
            ], className="col-md-4"),
            html.Div(id='task1_p', children=[
                html.P(
                    id="task1_insight",
                    children="List of Top 10 movies with the highest selected parameter for any year",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task1_bar_chart')
    ]),
    
    html.Div(id='task3_container', children=[
        html.H2(id='header_task3', style={'text-align': 'center'}),
        html.Div(id='task2_sub', children=[
            html.Div(id='task2_choice', children=[
                html.Label('Genre:'),
                dcc.Dropdown(id="slct_genre_task3",
                    options=genre_list,
                    multi=False,
                    value='Adventure',
                    clearable=False
                ),
                html.Label('Parameter:'),
                dcc.Dropdown(id="slct_col_task3",
                    options=col_list,
                    multi=False,
                    value="popularity",
                    clearable=False
                ),
            ], className="col-md-4"),
            html.Div(id='task3_p', children=[
                html.P(
                    id="task3_insight",
                    children="List of Top 10 movies with the highest selected parameter for any genre.",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task3_bar_chart')
    ]),
    
    html.Div(id='task4_container', children=[
        html.H2(id='header_task4', style={'text-align': 'center'}),
        html.Div(id='task4_sub', children=[
            html.Div(id='task4_choice', children=[
            html.Label('Parameter:'),
            dcc.Dropdown(id="slct_col_task4",
                options=col_list,
                multi=False,
                value="popularity",
                clearable=False,
            ),
            ], className="col-md-4"),
            html.Div(id='task4_p', children=[
                html.P(
                    id="task4_insight",
                    children="List of Top 10 production companies with the highest selected parameter.",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task4_bar_chart')
    ]),

       html.Div(id='task16_container', children=[
        html.H2(id='header_task16', style={'text-align': 'center'}),
        html.Div(id='task2_sub', children=[
            html.Div(id='task2_choice', children=[
                html.Label('Job:'),
                dcc.Dropdown(id="slct_job_task16",
                    options=job_list,
                    multi=False,
                    value='Actor',
                    clearable=False
                ),
                # html.Label('Parameter:'),
                # dcc.Dropdown(id="slct_col_task16",
                #     options=col_list,
                #     multi=False,
                #     value="popularity",
                #     clearable=False
                # ),
            ], className="col-md-4"),
            html.Div(id='task16_p', children=[
                html.P(
                    id="task16_insight",
                    children="List of Top 10 actors/directors with the highest average revenue generated.",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task16_bar_chart')
    ]),
])


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
#***Task1 callback***
@app.callback(
    [Output(component_id='header_task1', component_property='children'),
     Output(component_id='task1_bar_chart', component_property='figure')],
    [Input(component_id='slct_year_task1', component_property='value'),
    Input(component_id='slct_col_task1', component_property='value')]
)
def update_graph(slct_year, slct_col):
    print('task1 update', slct_year, slct_col)
    container = f"Top 10 Highest {col_label[slct_col]} Movies in {slct_year}"
    dff = df["task1"].copy()
    #filter col
    dff = dff[dff["year"] == slct_year].sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    #print("task 1 dff", dff)
    #filter rows
    #dff = dff[dff["Affected by"] == "Varroa_mites"]
    #text=slct_col,
    fig = px.bar(data_frame=dff, y='title', x=slct_col, orientation='h', text=slct_col, template="plotly_white", color_continuous_scale=colorscale, color=slct_col)
    fig.update_layout(
        #title="Plot Title",
        xaxis_title=col_label[slct_col],
        yaxis_title='Title',
    )
    return container, fig

#***Task3 callback***
@app.callback(
    [Output(component_id='header_task3', component_property='children'),
    Output(component_id='task3_bar_chart', component_property='figure')],
    [Input(component_id='slct_genre_task3', component_property='value'),
    Input(component_id='slct_col_task3', component_property='value')]
)
def update_graph(slct_genre, slct_col):
    print('task3 update', slct_genre, slct_col)
    container = f"Top 10 Highest {col_label[slct_col]} Movies in {slct_genre} (2000-2017)"
    dff = df["task3"].copy()
    #filter col
    dff = dff[dff["genre_name"] == slct_genre].sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    #print("task3_dff", dff)
    fig = px.bar(data_frame=dff, y='title', x=slct_col, orientation='h', text=slct_col, template="plotly_white", color_continuous_scale=colorscale, color=slct_col)
    fig.update_layout(
        #title="Plot Title",
        xaxis_title=col_label[slct_col],
        yaxis_title='Title',
    )
    return container, fig

#***Task4 callback***
@app.callback(
    [Output(component_id='header_task4', component_property='children'),
    Output(component_id='task4_bar_chart', component_property='figure')],
    Input(component_id='slct_col_task4', component_property='value')
)
def update_graph(slct_col):
    print('task4 update')
    container = f"Top 10 Highest {col_label[slct_col]} Production Companies of All Time"
    dff = df["task4"].copy()
    #filter col
    dff = dff.sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    #print("task4_dff", dff)
    fig = px.bar(data_frame=dff, y='production_company', x=slct_col, orientation='h', text=slct_col, template="plotly_white", color_continuous_scale=colorscale, color=slct_col)
    fig.update_layout(
        #title="Plot Title",
        xaxis_title=col_label[slct_col],
        yaxis_title='Production Company',
    )
    return container, fig

#***Task16 callback***
@app.callback(
    [Output(component_id='header_task16', component_property='children'),
    Output(component_id='task16_bar_chart', component_property='figure')],
    [Input(component_id='slct_job_task16', component_property='value')]
)
def update_graph(slct_job):
    print('task16 update', slct_job)
    container = f"Top 10 Highest Average Revenue Generating {slct_job}s of All Time"
    dff = df["task16"].copy()
    #filter col
    dff = dff[dff["job"] == slct_job].sort_values(by='avg_revenue', ascending=False).head(10).sort_values(by='avg_revenue', ascending=True)
    #print("task3_dff", dff)
    fig = px.bar(data_frame=dff, y='name', x='avg_revenue', orientation='h', text='avg_revenue', template="plotly_white", color_continuous_scale=colorscale, color='avg_revenue')
    fig.update_layout(
        xaxis_title='Average Revenue',
        yaxis_title='Name',
    )
    return container, fig