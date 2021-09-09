##############################################
#### Written By: SATYAKI DE               ####
#### Written On: 26-Jul-2021              ####
#### Modified On 26-Jul-2021              ####
####                                      ####
#### Objective: Calling Covid-19 API      ####
##############################################

import json
from clsConfig import clsConfig as cf
import requests
import logging
import time
import pandas as p
import clsL as cl

class clsCovidAPI:
    def __init__(self):
        self.url = cf.conf['URL']
        self.azure_cache = cf.conf['CACHE']
        self.azure_con = cf.conf['conType']
        self.type = cf.conf['appType']
        self.typVal = cf.conf['coList']
        self.max_retries = cf.conf['MAX_RETRY']

    def searchQry(self, varVa, debugInd):
        try:
            url = self.url
            api_cache = self.azure_cache
            api_con = self.azure_con
            type = self.type
            typVal = self.typVal
            max_retries = self.max_retries
            var = varVa
            debug_ind = debugInd
            cnt = 0

            df_M = p.DataFrame()

            # Initiating Log class
            l = cl.clsL()

            payload = {}

            strMsg = 'Input Countries: ' + str(typVal)
            logging.info(strMsg)

            headers = {}

            countryList = typVal.split(',')

            for i in countryList:
                # Failed case retry
                retries = 1
                success = False
                val = ''

                try:
                    while not success:
                        # Getting response from web service
                        try:
                            df_conv = p.DataFrame()

                            strCountryUrl = url + str(i).strip()

                            print('Country: ' + str(i).strip())
                            print('Url: ' + str(strCountryUrl))

                            str1 = 'Url: ' + str(strCountryUrl)
                            logging.info(str1)

                            response = requests.request("GET", strCountryUrl, headers=headers, params=payload)
                            ResJson = response.text

                            #jdata = json.dumps(ResJson)
                            RJson = json.loads(ResJson)

                            df_conv = p.io.json.json_normalize(RJson)
                            df_conv.drop(['data.timeline'], axis=1, inplace=True)
                            df_conv['DummyKey'] = 1
                            df_conv.set_index('DummyKey')

                            l.logr('1.df_conv_' + var + '.csv', debug_ind, df_conv, 'log')

                            # Extracting timeline part separately
                            Rjson_1 = RJson['data']['timeline']

                            df_conv2 = p.io.json.json_normalize(Rjson_1)
                            df_conv2['DummyKey'] = 1
                            df_conv2.set_index('DummyKey')
                            l.logr('2.df_conv_timeline_' + var + '.csv', debug_ind, df_conv2, 'log')

                            # Doing Cross Join
                            df_fin = df_conv.merge(df_conv2, on='DummyKey', how='outer')
                            l.logr('3.df_fin_' + var + '.csv', debug_ind, df_fin, 'log')

                            # Merging with the previous Country Code data
                            if cnt == 0:
                                df_M = df_fin
                            else:
                                d_frames = [df_M, df_fin]
                                df_M = p.concat(d_frames)

                            cnt += 1


                            strCountryUrl =  ''

                            if str(response.status_code)[:1] == '2':
                                success = True
                            else:
                                wait = retries * 2
                                print("retries Fail! Waiting " + str(wait) + " seconds and retrying!")
                                str_R1 = "retries Fail! Waiting " + str(wait) + " seconds and retrying!"
                                logging.info(str_R1)
                                time.sleep(wait)
                                retries += 1

                            # Checking maximum retries
                            if retries == max_retries:
                                success = True
                                raise  Exception

                        except Exception as e:
                            x = str(e)
                            print(x)
                            logging.info(x)

                            pass

                except Exception as e:
                    pass

            l.logr('4.df_M_' + var + '.csv', debug_ind, df_M, 'log')


            return df_M

        except Exception as e:

            x = str(e)
            print(x)

            logging.info(x)
            df = p.DataFrame()

            return df
