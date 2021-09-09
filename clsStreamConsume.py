##############################################
#### Written By: SATYAKI DE               ####
#### Written On: 26-Jul-2021              ####
#### Modified On 08-Sep-2021              ####
####                                      ####
#### Objective: Consuming Streaming data  ####
#### from Ably channels published by the  ####
#### callPredictCovidAnalysisRealtime.py  ####
####                                      ####
##############################################

import json
from clsConfig import clsConfig as cf
import requests
import logging
import time
import pandas as p
import clsL as cl

from ably import AblyRest

# Initiating Log class
l = cl.clsL()

class clsStreamConsume:
    def __init__(self):
        self.ably_id = str(cf.conf['ABLY_ID'])
        self.fileName = str(cf.conf['FinData'])

    def conStream(self, varVa, debugInd):
        try:
            ably_id = self.ably_id
            fileName = self.fileName

            var = varVa
            debug_ind = debugInd

            # Fetching the data
            client = AblyRest(ably_id)
            channel = client.channels.get('sd_channel')

            message_page = channel.history()

            # Counter Value
            cnt = 0

            # Declaring Global Data-Frame
            df_conv = p.DataFrame()

            for i in message_page.items:
                print('Last Msg: {}'.format(i.data))
                json_data = json.loads(i.data)

                # Converting JSON to Dataframe
                df = p.json_normalize(json_data)
                df.columns = df.columns.map(lambda x: x.split(".")[-1])

                if cnt == 0:
                    df_conv = df
                else:
                    d_frames = [df_conv, df]
                    df_conv = p.concat(d_frames)

                cnt += 1

            # Resetting the Index Value
            df_conv.reset_index(drop=True, inplace=True)


            # This will check whether the current load is happening
            # or not. Based on that, it will capture the old events
            # from cache.

            if df_conv.empty:
                df_conv = p.read_csv(fileName, index = True)
            else:
                l.logr(fileName, debug_ind, df_conv, 'log')

            return df_conv

        except Exception as e:

            x = str(e)
            print(x)

            logging.info(x)

            # This will handle the error scenaio as well.
            # Based on that, it will capture the old events
            # from cache.

            try:
                df_conv = p.read_csv(fileName, index = True)
            except:
                df = p.DataFrame()

            return df
