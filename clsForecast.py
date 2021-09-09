##############################################
#### Written By: SATYAKI DE               ####
#### Written On: 26-Jul-2021              ####
#### Modified On 26-Jul-2021              ####
####                                      ####
#### Objective: Calling Data Cleaning API ####
##############################################

import json
from clsConfig import clsConfig as cf
import requests
import logging
import time
import pandas as p
import clsL as cl

from prophet import Prophet

class clsForecast:
    def __init__(self):
        self.fnc = cf.conf['FNC']
        self.fnd = cf.conf['FND']
        self.tms = cf.conf['TMS']

    def forecastNewConfirmed(self, srcDF, debugInd, varVa):
        try:
            fnc = self.fnc
            tms = self.tms
            var = varVa
            debug_ind = debugInd
            countryISO = ''

            df_M = p.DataFrame()

            dfWork = srcDF

            # Initiating Log class
            l = cl.clsL()

            #Extracting the unique country name
            unqCountry = dfWork['CountryCode'].unique()

            for i in unqCountry:
                countryISO = i.strip()

            print('Country Name: ' + countryISO)

            df_Comm = dfWork[[tms, fnc]]
            l.logr('13.df_Comm_' + var + '.csv', debug_ind, df_Comm, 'log')

            # Aligning as per Prophet naming convention
            df_Comm.columns = ['ds', 'y']
            l.logr('14.df_Comm_Mod_' + var + '.csv', debug_ind, df_Comm, 'log')

            return df_Comm

        except Exception as e:

            x = str(e)
            print(x)

            logging.info(x)
            df = p.DataFrame()

            return df

    def forecastNewDead(self, srcDF, debugInd, varVa):
        try:
            fnd = self.fnd
            tms = self.tms
            var = varVa
            debug_ind = debugInd
            countryISO = ''

            df_M = p.DataFrame()

            dfWork = srcDF

            # Initiating Log class
            l = cl.clsL()

            #Extracting the unique country name
            unqCountry = dfWork['CountryCode'].unique()

            for i in unqCountry:
                countryISO = i.strip()

            print('Country Name: ' + countryISO)

            df_Comm = dfWork[[tms, fnd]]
            l.logr('17.df_Comm_' + var + '.csv', debug_ind, df_Comm, 'log')

            # Aligning as per Prophet naming convention
            df_Comm.columns = ['ds', 'y']
            l.logr('18.df_Comm_Mod_' + var + '.csv', debug_ind, df_Comm, 'log')

            return df_Comm

        except Exception as e:

            x = str(e)
            print(x)

            logging.info(x)
            df = p.DataFrame()

            return df
