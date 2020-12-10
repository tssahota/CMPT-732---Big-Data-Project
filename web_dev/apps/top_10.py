import pandas as pd
import plotly.express as px
#import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

from app import app

# ------------------------------------------------------------------------------
# Import and clean data (importing csv into pandas)
path_list = ['./apps/analysis_data/task1', './apps/analysis_data/task2', './apps/analysis_data/task3', './apps/analysis_data/task4'] # use your path
task_list = ['task1', 'task2', 'task3', 'task4']
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
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
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
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
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
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task4_bar_chart')
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
    container = f"Top 10 {col_label[slct_col]} Movies in {slct_year}"
    dff = df["task1"].copy()
    #filter col
    dff = dff[dff["year"] == slct_year].sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    #print("task 1 dff", dff)
    #filter rows
    #dff = dff[dff["Affected by"] == "Varroa_mites"]
    #text=slct_col,
    fig = px.bar(data_frame=dff, y='title', x=slct_col, orientation='h', text=slct_col, template="ggplot2", color=slct_col )
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
    container = f"Top 10 {col_label[slct_col]} Movies in {slct_genre} (2000-2017)"
    dff = df["task3"].copy()
    #filter col
    dff = dff[dff["genre_name"] == slct_genre].sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    #print("task3_dff", dff)
    fig = px.bar(data_frame=dff, y='title', x=slct_col, orientation='h', text=slct_col, template="ggplot2", color=slct_col)
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
    container = f"Top 10 {col_label[slct_col]} Production Companies of all-time"
    dff = df["task4"].copy()
    #filter col
    dff = dff.sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    #print("task4_dff", dff)
    fig = px.bar(data_frame=dff, y='production_company', x=slct_col, orientation='h', text=slct_col, template="ggplot2", color=slct_col)
    fig.update_layout(
        #title="Plot Title",
        xaxis_title=col_label[slct_col],
        yaxis_title='Production Company',
    )
    return container, fig
