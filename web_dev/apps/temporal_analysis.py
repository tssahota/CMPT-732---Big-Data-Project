import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

from app_temp import app

path_list = ['./apps/analysis_data/task2', './apps/analysis_data/task19'] # use your path
task_list = ['task2', 'task19']
df = {}
for i, path in enumerate(path_list):
    filenames = glob.glob(path+"/*.parquet")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_parquet(filename))
    # Concatenate all data into one DataFrame
    df[task_list[i]] = pd.concat(dfs, ignore_index=True)


#task2
col_label = {}
col_label['vote_average'] = 'TMDB critics rating (Max=10)'
col_label['avg_user_rating'] = 'Average User Rating (Max=5)'
col_label['popularity'] = 'Popularity'
col_label['profit'] = 'Profit'

col_list = [{"label": col_label["popularity"], "value": "popularity"},
            {"label": col_label["profit"], "value": "profit"},
            {"label": col_label['vote_average'], "value": "vote_average"},
            {"label": col_label['avg_user_rating'], "value": "avg_user_rating"}]


#task19
col_names = df['task19'].columns
task19_col_list = []
for col_name in col_names:
    task19_col_list.append({"label": col_name, "value": col_name})

column_name_list = df['task19'].columns.tolist()

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
                    id="task2_insight",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
                ),
            ], className="col-md-8"),
        ], className="row"),
        dcc.Graph(id='task2_bar_chart')
    ]),

     html.Div(id='task19_container', children=[
        html.H2(id='header_task19', style={'text-align': 'center'}, children='Quantitative Features Over the Years'),
        html.Div(id='task19_p', children=[
                html.P(
                    id="task19_insight",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
                ),
        ]),
        html.Div(id='task19_sub', children=[
            html.Div(id='task19_l', children=[
                html.Label('Parameter:'),
                dcc.Dropdown(
                    id="slct_col_task19_l",
                    options=task19_col_list,
                    multi=False,
                    value="vote_count",
                    clearable=False,
                ),
                dcc.Graph(id='task19_chart_l')
            ], className="col-md-6"),
            html.Div(id='task19_r', children=[
                html.Label('Parameter:'),
                dcc.Dropdown(
                    id="slct_col_task19_r",
                    options=task19_col_list,
                    multi=False,
                    value="vote_average",
                    clearable=False,
                ),
                dcc.Graph(id='task19_chart_r')
            ], className="col-md-6"),
        ], className="row"),
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
    fig = px.bar(data_frame=dff, y=slct_col, x=dff.index, orientation='v', text='genre_name', template="plotly_white", color='genre_name')
    fig.update_layout(
        #title="Plot Title",
        xaxis_title='Year',
        yaxis_title=col_label[slct_col],
    )
    return container, fig

#***Task19 callback l***
@app.callback(
    Output(component_id='task19_chart_l', component_property='figure'),
    [Input(component_id='slct_col_task19_l', component_property='value')]
)
def update_graph(slct_col):
    #task19
    fig = px.line(
        df['task19'].sort_values(by=['year']),
            x='year', 
            y=slct_col
        )
    return fig

#***Task19 callback r***
@app.callback(
    Output(component_id='task19_chart_r', component_property='figure'),
    [Input(component_id='slct_col_task19_r', component_property='value')]
)
def update_graph(slct_col):
    fig = px.line(
    df['task19'].sort_values(by=['year']), 
        x='year',
        y=slct_col,
    )
    return fig