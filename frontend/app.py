# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import psycopg2
import pandas as pd


#image = 'stack-overflow.jpg'
#encoded_image = base64.b64encode(open(image, 'rb').read())
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

df1 = pd.DataFrame()
def load_data(query):
    conn = psycopg2.connect(host='YOUR_HOST', dbname='YOUR_DB', user='USERNAME',password='PASSWORD')
    #curr = conn.cursor()
    #tags_query = " SELECT DISTINCT * FROM df_Tags_Avg ;"
    df = pd.read_sql(query, conn)
    return df

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(html.A(str(int(dataframe.iloc[i][col])), href='https://stackoverflow.com/questions/'+str(int(dataframe.iloc[i][col])))) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))], style={'border-collapse':False}
    )



app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#tags_query = " SELECT DISTINCT * FROM df_Tags_Avg ;"
top_tags = ['java','python','scala','javascript','c','git'] #load_data(tags_query)['_PostTag']
app.layout = html.Div([

    html.Div([ html.H1(children='Spam Stack')
               ], className= 'twelve columns', style={'textAlign':'center'}) ,


    html.Div([ dcc.Dropdown( id = 'input-tag',
                             options = [{ 'label': val , 'value': val} for val in top_tags],
                             value='java')


    ]),
#html.Button(id='tag-button', children='Submit', value='python'),



    html.Div([#id = output-container
            html.Div([dcc.Graph(id='g1')],className='ten columns'),
            html.Div([html.Div(id='table-container',)] #className='table')]
             , className='two columns'),
            #dcc.Graph(id='g2')

    ]),



])

@app.callback(
    [Output(component_id='g1', component_property='figure'),
    Output(component_id='table-container', component_property='children')],
    [Input(component_id='input-tag', component_property='value')])
    #[State(component_id='input-tag', component_property='value')])

def make_query(input_tag):
    custom_query_1 = " SELECT  *  from " +input_tag+ "_avg_score order by "+input_tag+"_avg_score.\"_ParentId\"  LIMIT 1000; "
    df1 = load_data(custom_query_1)
    custom_query_2 = " SELECT * from " + input_tag + "_avg_new LIMIT 1000;"
    df2 = load_data(custom_query_2)
    custom_query_3 = " SELECT "+input_tag+"_improv.\"_ParentId\" as \"Posts\" from " + input_tag + "_improv LIMIT 7;"
    df3 = load_data(custom_query_3)
    data_table = generate_table(df3)
    return [{'data': [{'x': df1['_ParentId'], 'y': df1['_avgscore'],'mode':'markers', 'name':'Before'},
                      {'x': df2['_ParentId'], 'y': df2['_avgscore'],'mode':'markers','opacity':0.7,'name':'After'}],
         'layout': {'xaxis': {'title': 'Post Id'}, 'yaxis': {'title': 'Score'}}},data_table]




if __name__ == '__main__':
    app.run_server(debug=True,host="0.0.0.0",port=80)
