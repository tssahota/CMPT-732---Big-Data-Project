import pandas as pd
import plotly.express as px
#import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import glob

from app_temp import app

path_list = ['./apps/analysis_data/task18', './apps/analysis_data/task15'] # use your path
task_list = ['task18', 'task15']
df = {}
for i, path in enumerate(path_list):
    filenames = glob.glob(path+"/*.parquet")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_parquet(filename))
    # Concatenate all data into one DataFrame
    df[task_list[i]] = pd.concat(dfs, ignore_index=True)

print(df['task15'])


col_names = df['task18'].columns
# col_label = {}
# col_label['vote_average'] = 'TMDB critics rating (Max=10)'
# col_label['avg_user_rating'] = 'Average User Rating (Max=5)'
# col_label['popularity'] = 'Popularity'
# col_label['profit'] = 'Profit'
# {"label": col_label["popularity"], "value": "popularity"},
#             {"label": col_label["profit"], "value": "profit"},
#             {"label": col_label['vote_average'], "value": "vote_average"},
#             {"label": col_label['avg_user_rating'], "value": "avg_user_rating"}
col_list = []
for col_name in col_names:
    col_list.append({"label": col_name, "value": col_name})


layout = html.Div([
    html.Div(id='task18_container', children=[
        html.H2(id='header_task18', style={'text-align': 'center'}, children='Data Distribution & Outliers'),
        html.P(
            id="task18_insight",
            children="† Deaths are classified using the International Classification of Diseases, \
            Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
            cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
            (undetermined intent).",
        ),
        html.Div(id='task18_sub', children=[
            html.Div(id='task18_l', children=[
                html.Label('Parameter:'),
                dcc.Dropdown(
                    id="slct_col_task18_l",
                    options=col_list,
                    multi=False,
                    value="vote_count",
                    clearable=False,
                ),
                dcc.Graph(id='task18_chart_l')
            ], className="col-md-6"),
            html.Div(id='task18_r', children=[
                html.Label('Parameter:'),
                dcc.Dropdown(
                    id="slct_col_task18_r",
                    options=col_list,
                    multi=False,
                    value="vote_average",
                    clearable=False,
                ),
                dcc.Graph(id='task18_chart_r')
            ], className="col-md-6"),
        ], className="row"),
    ]),
     html.Div(id='task15_container', children=[
        html.H2(id='header_task15', style={'text-align': 'center'}, children='Correlation Heatmap'),
        html.P(
            id="task15_insight",
            children="† Deaths are classified using the International Classification of Diseases, \
            Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
            cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
            (undetermined intent).",
        ),
        html.P("Included:"),
        dcc.Checklist(
            id='task15_parameters',
            options=[{'label': x, 'value': x} 
                    for x in df['task15'].columns],
            value=df['task15'].columns.tolist(),
        ),
        dcc.Graph(id="task15_heatmap"),
     ]),
])

#***Task18 callback l***
@app.callback(
    Output(component_id='task18_chart_l', component_property='figure'),
    [Input(component_id='slct_col_task18_l', component_property='value')]
)
def update_graph(slct_col):
    fig = px.violin(
        df['task18'],
        y=slct_col,
        box=True,
        points='all',
    )
    return fig

#***Task18 callback r***
@app.callback(
    Output(component_id='task18_chart_r', component_property='figure'),
    [Input(component_id='slct_col_task18_r', component_property='value')]
)
def update_graph(slct_col):
    fig = px.violin(
        df['task18'],
        y=slct_col,
        box=True,
        points='all',
    )
    return fig

#***Task15 callback***
@app.callback(
    Output("task15_heatmap", "figure"), 
    [Input("task15_parameters", "value")])
def filter_heatmap(cols):
    fig = px.imshow(df['task15'][cols],
                labels=dict(x="Parameter X", y="Parameters Y"),
                x=df['task15'].columns.tolist(),
                y=df['task15'].columns.tolist()
               )
    fig.update_xaxes(side="top")
    return fig
