import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from app_temp import app
# from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel


conf = SparkConf().setAppName("PySpark App").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("ui").getOrCreate()
model = PipelineModel.load('./apps/best_model/bestModel')

#spark = SparkSession.builder.appName("task").getOrCreate()
#spark.driver.allowMultipleContexts = True

# director_options=[
#         {'label': 'New York City', 'value': 'NYC'},
#         {'label': 'Montreal', 'value': 'MTL'},
#         {'label': 'San Francisco', 'value': 'SF'},
#         {'label': 'adsfasdfasdf', 'value': 'Sd'},
#         {'label': 'sgdfgdgfgadssasd', 'value': 'Sg'},]

# genre_options=[
#     {'label': 'New York City', 'value': 'NYC'},
#     {'label': 'Montreal', 'value': 'MTL'},
#     {'label': 'San Francisco', 'value': 'SF'},
#     {'label': 'adsfasdfasdf', 'value': 'Sd'},
#     {'label': 'sgdfgdgfgadssasd', 'value': 'Sg'},
# ]

# cast_options=[
#     {'label': 'New York City', 'value': 'NYC'},
#     {'label': 'Montreal', 'value': 'MTL'},
#     {'label': 'San Francisco', 'value': 'SF'},
#     {'label': 'adsfasdfasdf', 'value': 'Sd'},
#     {'label': 'sgdfgdgfgadssasd', 'value': 'Sg'},
# ]

#features = ['budget', 'genre', 'director', 'cast', 'runtime', 'release_date']
features = ['budget', 'vote_count','popularity', 'keyword_power', 'youtube_views', 'youtube_likes']

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
                        min=1,
                        style={'width': '100%'}
                    ),
                ]),
                # html.Div(children=[
                #     html.Label('Vote Average'),
                #     dbc.Input(
                #         id="vote_average",
                #         placeholder="Vote Average",
                #         type='number',
                #         min=0,
                #         max=5,
                #         style={'width': '100%'}
                #     ),
                # ], style={'margin-top': '5px'}),
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
                html.Div(children=[
                    html.Label('Keyword Power'),
                    dbc.Input(
                        id="keyword_power",
                        placeholder="Keyword Power",
                        type='number',
                        min=0,
                        style={'width': '100%'}
                    ),
                ], style={'margin-top': '5px'}),
                html.Div(children=[
                    html.Label('Youtube Views'),
                    dbc.Input(
                        id="youtube_views",
                        placeholder="Youtube Views",
                        type='number',
                        min=0,
                        style={'width': '100%'}
                    ),
                ], style={'margin-top': '5px'}),
                html.Div(children=[
                    html.Label('Youtube Likes'),
                    dbc.Input(
                        id="youtube_likes",
                        placeholder="Youtube Likes",
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
                # html.Div(children=[
                #     html.Label('Run Time'),
                #     dbc.Input(
                #         id="runtime",
                #         placeholder="run time",
                #         type='number',
                #         min=0,
                #         style={'width': '100%'}
                #     ),
                # ], style={'margin-top': '5px'}),
                # html.Label('Planned Release Date', style={'width': '100%'}),
                #     dcc.DatePickerSingle(
                #         id="release_date",
                #         clearable=True,
                #         with_portal=True,
                #         display_format='MMM D YYYY',
                #     ),
                html.Div(children=[
                ], style={'margin-top': '5px'}),
                html.Div(id='result_div', children=[
                    html.Label('Prediction'),
                    dbc.InputGroup([
                        dbc.InputGroupAddon("$", addon_type="prepend"),
                                            dcc.Input(
                        id="predict_result",
                        placeholder="Predict Result",
                        readOnly=True,
                        style={'width': '80%'}
                    ),
                        ],
                    ),
                    dbc.Button("Predict", id="predict_btn", color="primary", className="ml-5 float-right")
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
features = ['budget', 'vote_count','popularity', 'keyword_power', 'youtube_views', 'youtube_likes']

@app.callback(
    Output(component_id="predict_result", component_property="value"),
    [Input(component_id="budget", component_property="value"),
    Input(component_id="vote_count", component_property="value"),
    Input(component_id="popularity", component_property="value"), 
    Input(component_id="keyword_power", component_property="value"),
    Input(component_id="youtube_views", component_property="value"),
    Input(component_id="youtube_likes", component_property="value"),
    Input("predict_btn", "n_clicks")
    ],
)
def predict_features(budget, vote_count,popularity, keyword_power, youtube_views, youtube_likes, n):
    # if release_date is not None:
    #     date_object = date.fromisoformat(release_date)
    #     date_string = date_object.strftime('%B %d, %Y')
    #     print (string_prefix + date_string)
    if n:
        features_res = {}
        temp = [budget, vote_count, popularity, keyword_power, youtube_views, youtube_likes]
        label = ['Budget', 'Vote Count', 'Popularity', 'Keyword Power', 'Youtube Views', 'Youtube Likes']
        mis_list = []
        for i, feature in enumerate(features):
            # if i == len(temp)-1:
            #     #print(temp[i])
            #     features_res[feature] = datetime.strptime(temp[i], '%Y-%m-%d').timetuple().tm_yday
            # else:
            if temp[i] == None:
                mis_list.append(label[i])
            features_res[feature] = temp[i]
        print(features_res)
        temp_res = {'budget': 1, 'vote_count':2, 'popularity':3, 'keyword_power':4, 'youtube_views':5, 'youtube_likes':6}
        sc_df = spark.createDataFrame(Row(**i) for i in [temp_res])
        sc_df.show()

        # predictions = model.transform(sc_df)
        # predictions.show()
        # prediction = predictions.collect()[0].asDict()['prediction']

        #spark_df = spark.createDataFrame([Row(features_res)])
        #print(spark_df.schema)
        #spark_df.show()
        #update predict result
        if mis_list:
            err_msg = 'Error: Please fill in '
            for ele in mis_list:
                err_msg = err_msg + ele + ', '
            return err_msg+' and try again.'
        else:
            return 123123123
    else:
        return None
