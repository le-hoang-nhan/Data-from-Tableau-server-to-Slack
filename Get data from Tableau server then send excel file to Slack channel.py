import os
from pathlib import Path
from dotenv import load_dotenv
import tableauserverclient as TSC
import pandas as pd

#access to Tableau
tableau_auth = TSC.TableauAuth()
server = TSC.Server('https://........online.tableau.com/', use_server_version=True)
request_options = TSC.RequestOptions(pagesize=1000)
server.auth.sign_in(tableau_auth)

#get the view id
with server.auth.sign_in(tableau_auth):
    for view in [v for v in TSC.Pager(server.views) if v.name.startswith('........ subscription base')]:
        #print(view.name, ': ', view.id)
        view_id = view.id

#Save an excel file 
with server.auth.sign_in(tableau_auth):
    view= server.views.get_by_id(view_id)
    #print(view.name)
    server.views.populate_csv(view)
    with open ('./........... subscription base (temporary file).csv', 'wb') as v:
        v.write(b''.join(view.csv))


dataframe =pd.read_csv('./.......... subscription base (temporary file).csv')

#Transformation
dataframe=dataframe.pivot(index='Month of End period', columns=['Product name', 'Measure Names'], values='Measure Values')
dataframe.index = pd.to_datetime(dataframe.index)
dataframe= dataframe.sort_index(ascending=True)

#save data
dataframe.to_csv('........ subscription base.csv')  

#setup libraries for Slack bot
from argparse import FileType
import slack

SLACK_TOKEN="............Bq8420F"
# Authenticate to the Slack API via the generated token
#client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
client = slack.WebClient(token=SLACK_TOKEN)

client.files_upload(
        channels = "......KYKS",
        initial_comment = "Hi @..........subscription base.csv'.",
        filename = "excel file of ........ subscription base",
        file= '....... subscription base.csv', 
        FileType="CSV")