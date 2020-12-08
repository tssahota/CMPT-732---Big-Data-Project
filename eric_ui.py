import pandas as pd
import plotly.express as px  # (version 4.7.0)
import plotly.graph_objects as go

import dash  # (version 1.12.0) 
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import glob

app = dash.Dash(__name__)

# ------------------------------------------------------------------------------
# Import and clean data (importing csv into pandas)
path = './analysis_data/year_return' # use your path
filenames = glob.glob(path+"/*.parquet")
dfs = []
for filename in filenames:
    #dfs.append(pd.read_csv(filename, sep='|', header=0))
    dfs.append(pd.read_parquet(filename))
# Concatenate all data into one DataFrame
df = pd.concat(dfs, ignore_index=True)
#print(df)
# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([
    html.H1("Web Application Dashboards with Dash", style={'text-align': 'center'}),
    dcc.Dropdown(id="slct_year",
                 options=[
                     {"label": "2017", "value": 2017},
                     {"label": "2016", "value": 2016},
                     {"label": "2015", "value": 2015},
                     {"label": "2014", "value": 2014},
                     {"label": "2013", "value": 2013},
                     {"label": "2012", "value": 2012},
                     {"label": "2011", "value": 2011},
                     {"label": "2010", "value": 2010},
                     {"label": "2009", "value": 2009},
                     {"label": "2008", "value": 2008}],
                 multi=False,
                 value=2017,
                 clearable=False,
                 style={'width': "50%"}
                 ),
    dcc.Dropdown(id="slct_col",
                 options=[
                     {"label": "Popularity", "value": "popularity"},
                     {"label": "Return", "value": "return"},
                     {"label": "Vote Average", "value": "vote_average"}],
                 multi=False,
                 value="popularity",
                 clearable=False,
                 style={'width': "50%"}
                 ),
    html.Div(id='output_container'),
    html.Br(),

    dcc.Graph(id='bar_chart')

])


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='output_container', component_property='children'),
     Output(component_id='bar_chart', component_property='figure')],
    [Input(component_id='slct_year', component_property='value'),
    Input(component_id='slct_col', component_property='value')]
)
def update_graph(slct_year, slct_col):
    print(slct_year, slct_col)

    container = f"Showing the movie ranking by {slct_col} in {slct_year}"

    dff = df.copy()
    #filter col
    dff = dff[dff["year"] == slct_year].sort_values(by=slct_col, ascending=False).head(10).sort_values(by=slct_col, ascending=True)
    print("dff", dff)
    #filter rows
    #dff = dff[dff["Affected by"] == "Varroa_mites"]
    fig = px.bar(data_frame=dff, y='title', x=slct_col, orientation='h', text=slct_col, template="plotly_dark" )
    return container, fig


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True)