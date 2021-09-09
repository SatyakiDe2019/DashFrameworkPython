##############################################
#### Written By: SATYAKI DE               ####
#### Written On: 08-Sep-2021              ####
#### Modified On 08-Sep-2021              ####
####                                      ####
#### Objective: This is the main script   ####
#### to invoke dashboard after consuming  ####
#### streaming real-time predicted data   ####
#### using Facebook API & Ably message Q. ####
####                                      ####
#### This script will show the trend      ####
#### comparison between major democracies ####
#### of the world.                        ####
####                                      ####
##############################################

import datetime

import dash
from dash import dcc
from dash import html
import plotly
from dash.dependencies import Input, Output
from ably import AblyRest

from clsConfig import clsConfig as cf
import pandas as p

# Main Class to consume streaming
import clsStreamConsume as ca

import numpy as np

# Create the instance of the Covid API Class
x1 = ca.clsStreamConsume()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
    html.Div([
        html.H1("Covid-19 Trend Dashboard",
                        className='text-center text-primary mb-4'),
        html.H5(children='''
            Dash: Covid-19 Trend - (Present Vs Future)
        '''),
        html.P("Covid-19: New Confirmed Cases:",
               style={"textDecoration": "underline"}),
        dcc.Graph(id='live-update-graph-1'),
        html.P("Covid-19: New Death Cases:",
               style={"textDecoration": "underline"}),
        dcc.Graph(id='live-update-graph-2'),
        dcc.Interval(
            id='interval-component',
            interval=5*1000, # in milliseconds
            n_intervals=0
        )
    ], className="row", style={'marginBottom': 10, 'marginTop': 10})
)

def to_OptimizeString(row):
    try:
        x_str = str(row['Year_Mon'])

        dt_format = '%Y%m%d'
        finStr = x_str + '01'

        strReportDate = datetime.datetime.strptime(finStr, dt_format)

        return strReportDate

    except Exception as e:
        x = str(e)
        print(x)

        dt_format = '%Y%m%d'
        var = '20990101'

        strReportDate = datetime.strptime(var, dt_format)

        return strReportDate

def fetchEvent(var1, DInd):
    try:
        # Let's pass this to our map section
        iDF_M = x1.conStream(var1, DInd)

        # Converting Year_Mon to dates
        iDF_M['Year_Mon_Mod']= iDF_M.apply(lambda row: to_OptimizeString(row), axis=1)

        # Dropping old columns
        iDF_M.drop(columns=['Year_Mon'], axis=1, inplace=True)

        #Renaming new column to old column
        iDF_M.rename(columns={'Year_Mon_Mod':'Year_Mon'}, inplace=True)

        return iDF_M

    except Exception as e:
        x = str(e)
        print(x)

        iDF_M = p.DataFrame()

        return iDF_M

# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph-1', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    try:
        var1 = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        print('*' *60)
        DInd = 'Y'

        # Let's pass this to our map section
        retDF = fetchEvent(var1, DInd)

        # Create the graph with subplots
        #fig = plotly.tools.make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.3, horizontal_spacing=0.2)
        fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.3, horizontal_spacing=0.2)

        # Routing data to dedicated DataFrame
        retDFNC = retDF.loc[(retDF['Status'] == 'NewConfirmed')]

        # Adding different chart into one dashboard
        # First Use Case - New Confirmed
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.Brazil,'type':'scatter','name':'Brazil'},1,1)
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.Canada,'type':'scatter','name':'Canada'},1,1)
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.Germany,'type':'scatter','name':'Germany'},1,1)
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.India,'type':'scatter','name':'India'},1,1)
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.Indonesia,'type':'scatter','name':'Indonesia'},1,1)
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.UnitedKingdom,'type':'scatter','name':'United Kingdom'},1,1)
        fig.append_trace({'x':retDFNC.Year_Mon,'y':retDFNC.UnitedStates,'type':'scatter','name':'United States'},1,1)

        return fig

    except Exception as e:
        x = str(e)
        print(x)

        # Create the graph with subplots
        fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.2)

        fig['layout']['margin'] = {
            'l': 30, 'r': 10, 'b': 30, 't': 10
        }
        fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}

        return fig
      
# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph-2', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    try:
        var1 = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        print('*' *60)
        DInd = 'Y'

        # Let's pass this to our map section
        retDF = fetchEvent(var1, DInd)

        # Create the graph with subplots
        #fig = plotly.tools.make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.3, horizontal_spacing=0.2)
        fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.3, horizontal_spacing=0.2)

        # Routing data to dedicated DataFrame
        retDFND = retDF.loc[(retDF['Status'] == 'NewDeaths')]

        # Adding different chart into one dashboard
        # Second Use Case - New Confirmed
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.Brazil,'type':'bar','name':'Brazil'},1,1)
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.Canada,'type':'bar','name':'Canada'},1,1)
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.Germany,'type':'bar','name':'Germany'},1,1)
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.India,'type':'bar','name':'India'},1,1)
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.Indonesia,'type':'bar','name':'Indonesia'},1,1)
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.UnitedKingdom,'type':'bar','name':'United Kingdom'},1,1)
        fig.append_trace({'x':retDFND.Year_Mon,'y':retDFND.UnitedStates,'type':'bar','name':'United States'},1,1)

        return fig

    except Exception as e:
        x = str(e)
        print(x)

        # Create the graph with subplots
        fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.2)

        fig['layout']['margin'] = {
            'l': 30, 'r': 10, 'b': 30, 't': 10
        }
        fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}

        return fig

if __name__ == '__main__':
    app.run_server(debug=True)
