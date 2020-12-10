import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

from app import app

path_list = ['./apps/analysis_data/task8', './apps/analysis_data/task11'] # use your path
task_list = ['task8', 'task11']
df = {}

for i, path in enumerate(path_list):
    filenames = glob.glob(path+"/*.parquet")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_parquet(filename))
    # Concatenate all data into one DataFrame
    df[task_list[i]] = pd.concat(dfs, ignore_index=True)

col_label = {}
col_label['vote_average'] = 'TMDB critics rating (Max=10)'
col_label['avg_user_rating'] = 'Average User Rating (Max=5)'
col_label['popularity'] = 'Popularity'
col_label['profit'] = 'Profit'


col_list = [{"label": col_label["popularity"], "value": "popularity"},
            {"label": col_label["profit"], "value": "profit"},
            {"label": col_label['vote_average'], "value": "vote_average"},
            {"label": col_label['avg_user_rating'], "value": "avg_user_rating"}]

print(df)
layout = html.Div([
    html.Div(id='task8_container', children=[
        html.H2(id='header_task8', style={'text-align': 'center'}),
        html.Div(id='task8_sub', children=[
            html.Div(id='task8_choice', children=[
                html.Label('X-axis:'),
                dcc.Dropdown(id="slct_colx_task8",
                            options=col_list,
                            multi=False,
                            value='avg_user_rating',
                            clearable=False,
                            ),
                html.Label('Y-axis:'),
                dcc.Dropdown(id="slct_coly_task8",
                            options=col_list,
                            multi=False,
                            value="popularity",
                            clearable=False,
                            ),
            ], className="col-md-4"),
            html.Div(id='task8_p', children=[
                html.P(
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).***count of language is the size of bubble here.***",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task8_bubble_chart')
    ]),

    html.Div(id='task11_container', children=[
        html.H2(id='header_task11', style={'text-align': 'center'}),
        html.Div(id='task11_sub', children=[
            html.Div(id='task11_choice', children=[
                html.Label('X-axis:'),
                dcc.Dropdown(id="slct_colx_task11",
                            options=col_list,
                            multi=False,
                            value='avg_user_rating',
                            clearable=False,
                            ),
                html.Label('Y-axis:'),
                dcc.Dropdown(id="slct_coly_task11",
                            options=col_list,
                            multi=False,
                            value="popularity",
                            clearable=False,
                            ),
            ], className="col-md-4"),
            html.Div(id='task11_p', children=[
                html.P(
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).***count of collection is the size of bubble here.***",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task11_bubble_chart')
    ]),

])

#******task8 ******
@app.callback(
    [Output(component_id='header_task8', component_property='children'),
     Output(component_id='task8_bubble_chart', component_property='figure')],
    [Input(component_id='slct_colx_task8', component_property='value'),
    Input(component_id='slct_coly_task8', component_property='value')]
)
def update_graph(slct_colx, slct_coly):
    print('task8 update', slct_colx, slct_coly)
    container = f"Original languages {col_label[slct_colx]} vs {col_label[slct_coly]}"
    dff = df["task8"].copy()
    #figure
    fig = px.scatter(dff, x=slct_colx, y=slct_coly,
	    size="count", color="count",
        hover_name="language", log_x=True,
        text="language",
        size_max=60, template="ggplot2",
    )
    fig.update_layout(
        #title="Plot Title",
        xaxis_title=col_label[slct_colx],
        yaxis_title=col_label[slct_coly],
    )
    return container, fig

#******task11 ******
@app.callback(
    [Output(component_id='header_task11', component_property='children'),
     Output(component_id='task11_bubble_chart', component_property='figure')],
    [Input(component_id='slct_colx_task11', component_property='value'),
    Input(component_id='slct_coly_task11', component_property='value')]
)
def update_graph(slct_colx, slct_coly):
    print('task11 update', slct_colx, slct_coly)
    container = f"{col_label[slct_colx]} vs {col_label[slct_coly]}"
    dff = df["task11"].copy()
    #figure
    fig = px.scatter(dff, x=slct_colx, y=slct_coly,
	    size="count", color="count",
        hover_name="collection_name", log_x=True,
        size_max=60, template="ggplot2",
    )
    fig.update_layout(
        #title="Plot Title",
        xaxis_title=col_label[slct_colx],
        yaxis_title=col_label[slct_coly],
    )
    return container, fig