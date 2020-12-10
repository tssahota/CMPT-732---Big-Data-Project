import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from PIL import Image

from app import app

path_list = ['./apps/analysis_data/task6', './apps/analysis_data/task7'] # use your path
task_list = ['task6', 'task7']
df = {}
for i, path in enumerate(path_list):
    filenames = glob.glob(path+"/*.parquet")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_parquet(filename))
    # Concatenate all data into one DataFrame
    df[task_list[i]] = pd.concat(dfs, ignore_index=True)

#******Task 6 fig6******
try: 
    img6 = Image.open('./img/task6_Word_Cloud.png')
except:
    df['task6'] = df['task6']
    df['task6'] = df['task6'].astype('str')
    title_wordcloud = WordCloud(stopwords=STOPWORDS, background_color='white', height=2000, width=4000).generate(' '.join((df['task6']['title'])))
    plt.figure(figsize=(16,8))
    plt.imshow(title_wordcloud)
    plt.axis('off') # to off the axis of x and y
    plt.savefig('./img/task6_Word_Cloud.png')
    img6 = Image.open('./img/task6_Word_Cloud.png')
# Constants
img_width = 1600
img_height = 900
scale_factor = 0.8
fig6 = go.Figure()

# Add invisible scatter trace.
# This trace is added to help the autoresize logic work.
fig6.add_trace(
    go.Scatter(
        x=[0, img_width * scale_factor],
        y=[0, img_height * scale_factor],
        mode="markers",
        marker_opacity=0
    )
)
# Configure axes
fig6.update_xaxes(
    visible=False,
    range=[0, img_width * scale_factor]
)

fig6.update_yaxes(
    visible=False,
    range=[0, img_height * scale_factor],
    # the scaleanchor attribute ensures that the aspect ratio stays constant
    scaleanchor="x"
)

# Add image
fig6.add_layout_image(
    dict(
        x=0,
        sizex=img_width * scale_factor,
        y=img_height * scale_factor,
        sizey=img_height * scale_factor,
        xref="x",
        yref="y",
        opacity=1.0,
        layer="below",
        sizing="stretch",
        source=img6)
)

# Configure other layout
fig6.update_layout(
    width=img_width * scale_factor,
    height=img_height * scale_factor,
    margin={"l": 0, "r": 0, "t": 0, "b": 0},
)


#******Task 7 fig7******
try: 
    img7 = Image.open('./img/task7_Word_Cloud.png')
except:
    df['task7'] = df['task7']
    df['task7'] = df['task7'].astype('str')
    overview_wordcloud = WordCloud(stopwords=STOPWORDS, background_color='white', height=2000, width=4000).generate(' '.join((df['task7']['keyword'])))
    plt.figure(figsize=(16,8))
    plt.imshow(overview_wordcloud)
    plt.axis('off') # to off the axis of x and y
    plt.savefig('./img/task7_Word_Cloud.png')
    img7 = Image.open('./img/task7_Word_Cloud.png')
# Constants
img_width = 1600
img_height = 900
scale_factor = 0.8
fig7 = go.Figure()

# Add invisible scatter trace.
# This trace is added to help the autoresize logic work.
fig7.add_trace(
    go.Scatter(
        x=[0, img_width * scale_factor],
        y=[0, img_height * scale_factor],
        mode="markers",
        marker_opacity=0
    )
)
# Configure axes
fig7.update_xaxes(
    visible=False,
    range=[0, img_width * scale_factor]
)

fig7.update_yaxes(
    visible=False,
    range=[0, img_height * scale_factor],
    # the scaleanchor attribute ensures that the aspect ratio stays constant
    scaleanchor="x"
)

# Add image
fig7.add_layout_image(
    dict(
        x=0,
        sizex=img_width * scale_factor,
        y=img_height * scale_factor,
        sizey=img_height * scale_factor,
        xref="x",
        yref="y",
        opacity=1.0,
        layer="below",
        sizing="stretch",
        source=img7)
)

# Configure other layout
fig7.update_layout(
    width=img_width * scale_factor,
    height=img_height * scale_factor,
    margin={"l": 0, "r": 0, "t": 0, "b": 0},
)


layout = html.Div([
    html.Div(id='task6_container', children=[
        html.H2(id='header_task6', style={'text-align': 'center'}, children='Most Common Words in Movie Titles'),
        html.Div(id='task6_p', children=[
            html.P(
                id="task6_insight",
                children="† Deaths are classified using the International Classification of Diseases, \
                Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                (undetermined intent).",
            ),
        ]),
        dcc.Graph(figure=fig6, style={"margin": "auto", "width" : '90%'})
    ]),

    html.Div(id='task7_container', children=[
        html.H2(id='header_task7', style={'text-align': 'center'}, children='Most Common Keyword in Movies'),
        html.Div(id='task7_p', children=[
            html.P(
                id="task7_insight",
                children="† Deaths are classified using the International Classification of Diseases, \
                Tenth Revision (ICD–10). Drug-poisoning deaths are defined as having ICD–10 underlying \
                cause-of-death codes X40–X44 (unintentional), X60–X64 (suicide), X85 (homicide), or Y10–Y14 \
                (undetermined intent).",
            ),
        ]),
        dcc.Graph(figure=fig7, style={"margin": "auto", "width" : '90%'})
    ]),
])