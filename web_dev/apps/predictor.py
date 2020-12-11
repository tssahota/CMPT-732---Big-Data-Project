import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from datetime import datetime
#from pyspark.sql import SparkSession, Row
#from pyspark import SparkConf
from app_temp import app

#SparkConf conf = new SparkConf()

#spark = SparkSession.builder.appName("task").getOrCreate()
#spark.driver.allowMultipleContexts = True

director_options=[
        {'label': 'New York City', 'value': 'NYC'},
        {'label': 'Montreal', 'value': 'MTL'},
        {'label': 'San Francisco', 'value': 'SF'},
        {'label': 'adsfasdfasdf', 'value': 'Sd'},
        {'label': 'sgdfgdgfgadssasd', 'value': 'Sg'},]

genre_options=[
    {'label': 'New York City', 'value': 'NYC'},
    {'label': 'Montreal', 'value': 'MTL'},
    {'label': 'San Francisco', 'value': 'SF'},
    {'label': 'adsfasdfasdf', 'value': 'Sd'},
    {'label': 'sgdfgdgfgadssasd', 'value': 'Sg'},
]

cast_options=[
    {'label': 'New York City', 'value': 'NYC'},
    {'label': 'Montreal', 'value': 'MTL'},
    {'label': 'San Francisco', 'value': 'SF'},
    {'label': 'adsfasdfasdf', 'value': 'Sd'},
    {'label': 'sgdfgdgfgadssasd', 'value': 'Sg'},
]

#features = ['budget', 'genre', 'director', 'cast', 'runtime', 'release_date']
features = ['budget', 'vote_average', 'vote_count','popularity', 'runtime', 'release_date']

layout = html.Div([
    html.Div(id='predictor_container', children=[
        html.H2(id='header_predictor', children='Box Office Predictor', style={'text-align': 'center'}),
        html.Div(id='predictor_sub', children=[
            html.Div(id='predictor_ui', children=[
                html.Div(children=[
                    html.Label('Budget'),
                    dbc.Input(
                        id="budget",
                        placeholder="Budget",
                        type='number',
                        min=0,
                        style={'width': '100%'}
                    ),
                ]),
                html.Div(children=[
                    html.Label('Vote Average'),
                    dbc.Input(
                        id="vote_average",
                        placeholder="Vote Average",
                        type='number',
                        min=0,
                        max=5,
                        style={'width': '100%'}
                    ),
                ], style={'margin-top': '5px'}),
                html.Div(children=[
                    html.Label('Vote Count'),
                    dbc.Input(
                        id="vote_count",
                        placeholder="Vote Count",
                        type='number',
                        min=0,
                        style={'width': '100%'}
                    ),
                ], style={'margin-top': '5px'}),
                html.Div(children=[
                    html.Label('Popularity'),
                    dbc.Input(
                        id="popularity",
                        placeholder="Popularity",
                        type='number',
                        min=0,
                        style={'width': '100%'}
                    ),
                ], style={'margin-top': '5px'}),
                # html.Div(children=[
                #     html.Label('Genre'),
                #     dcc.Dropdown(
                #         id='genre',
                #         options=genre_options,
                #         value=['MTL', 'NYC'],
                #         multi=True,
                #     )
                # ], style={'margin-top': '5px'}),
                # html.Div(children=[
                #     html.Label('Director'),
                #     dcc.Dropdown(
                #         id='director',
                #         options=director_options,
                #         value=['MTL'],
                #         multi=True,
                #     )
                # ], style={'margin-top': '5px'}),
                # html.Div(children=[
                #     html.Label('Cast'),
                #     dcc.Dropdown(
                #         id='cast',
                #         options=cast_options,
                #         value=['MTL'],
                #         multi=True,
                #     )
                # ], style={'margin-top': '5px'}),
                html.Div(children=[
                    html.Label('Run Time'),
                    dbc.Input(
                        id="runtime",
                        placeholder="run time",
                        type='number',
                        min=0,
                        style={'width': '100%'}
                    ),
                ], style={'margin-top': '5px'}),
                html.Div(children=[
                    html.Label('Planned Release Date', style={'width': '100%'}),
                    dcc.DatePickerSingle(
                        id="release_date",
                        clearable=True,
                        with_portal=True,
                        display_format='MMM D YYYY',
                    ),
                    dbc.Button("Predict", id="predict_btn", color="primary", className="ml-5 float-right"),
                ], style={'margin-top': '5px'}),
                html.Div(id='result_div', children=[
                    html.Label('Prediction'),
                    dbc.InputGroup([
                        dbc.InputGroupAddon("$", addon_type="prepend"),
                                            dcc.Input(
                        id="predict_result",
                        placeholder="Predict Result",
                        type='number',
                        readOnly=True,
                    ),
                        ],
                    ),

                ], style={'margin-top': '15px'}),
            ], className="col-md-8"),
            html.Div(id='predictor_p', children=[
                html.P(
                    id="description",
                    children="† Deaths are classified using the International Classification of Diseases, \
                    Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                    cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                    (undetermined intent).",
                ),
            ], className="col-md-4"),
        ], className="row"),

    ]),
])

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
# @app.callback(
#     Output(component_id="director", component_property="options"),
#     [Input(component_id="director", component_property="value"),],
# )
# def update_dropdown_options(values):
#     if values and len(values) == 1:
#         return [option for option in director_options if option["value"] in values]
#     else:
#         return director_options

# @app.callback(
#     Output(component_id="genre", component_property="options"),
#     [Input(component_id="genre", component_property="value"),],
# )
# def update_dropdown_options(values):
#     if values and len(values) == 3:
#         return [option for option in genre_options if option["value"] in values]
#     else:
#         return genre_options

# @app.callback(
#     Output(component_id="cast", component_property="options"),
#     [Input(component_id="cast", component_property="value"),],
# )
# def update_dropdown_options(values):
#     if values and len(values) == 3:
#         return [option for option in cast_options if option["value"] in values]
#     else:
#         return cast_options

@app.callback(
    Output(component_id="predict_result", component_property="value"),
    [Input(component_id="budget", component_property="value"),
    Input(component_id="vote_average", component_property="value"),
    Input(component_id="vote_count", component_property="value"), 
    Input(component_id="popularity", component_property="value"),
    Input(component_id="runtime", component_property="value"),
    Input(component_id="release_date", component_property="date"),
    Input("predict_btn", "n_clicks")
    ],
)
def predict_features(budget, vote_average, vote_count, popularity, runtime, release_date, n):
    # if release_date is not None:
    #     date_object = date.fromisoformat(release_date)
    #     date_string = date_object.strftime('%B %d, %Y')
    #     print (string_prefix + date_string)
    if n:
        features_res = {}
        temp = [budget, vote_average, vote_count, popularity, runtime, release_date]
        for i, feature in enumerate(features):
            if i == len(temp)-1:
                #print(temp[i])
                features_res[feature] = datetime.strptime(temp[i], '%Y-%m-%d').timetuple().tm_yday
            else: 
                features_res[feature] = temp[i]
        print(features_res)
        #spark_df = spark.createDataFrame([Row(features_res)])
        #print(spark_df.schema)
        #spark_df.show()
        #update predict result
        return 1000000000000000000034597839459835798345
    else:
        return None