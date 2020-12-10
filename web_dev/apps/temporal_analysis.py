import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

from app import app

path_list = ['./apps/analysis_data/task2'] # use your path
task_list = ['task2']
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

layout = html.Div([
    html.Div(id='task2_container', children=[
        html.H2(id='header_task2', style={'text-align': 'center'}),
        html.Div(id='task2_sub', children=[
            html.Div(id='task2_choice', children=[
                html.Label('Parameter:'),
                dcc.Dropdown(id="slct_col_task2",
                            options=col_list,
                            multi=False,
                            value="popularity",
                            clearable=False,
                            ),
            ], className="col-md-4"),
            html.Div(id='task2_p', children=[
                html.P(
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task2_bar_chart')
    ]),
])

#***Task2 callback***
@app.callback(
    [Output(component_id='header_task2', component_property='children'),
     Output(component_id='task2_bar_chart', component_property='figure')],
    [Input(component_id='slct_col_task2', component_property='value')]
)
def update_graph(slct_col):
    print('task2 update', slct_col)
    container = f"Top Average {col_label[slct_col]} Genre by year"
    dff = df["task2"].copy()
    #print(dff.groupby(['year']).max())
    #filter col
    dff = dff.groupby(['year']).max()
    #print(dff)
    fig = px.bar(data_frame=dff, y=slct_col, x=dff.index, orientation='v', text='genre_name', template="ggplot2")
    fig.update_layout(
        #title="Plot Title",
        xaxis_title='Year',
        yaxis_title=col_label[slct_col],
    )
    return container, fig
