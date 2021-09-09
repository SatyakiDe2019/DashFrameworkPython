###############################################################
####                                                       ####
#### Written By: Satyaki De                                ####
#### Written Date:  26-Jul-2021                            ####
#### Modified Date: 08-Sep-2021                            ####
####                                                       ####
#### Objective: This script will publish real-time         ####
#### streaming data coming out from a hosted API           ####
#### sources using another popular third-party service     ####
#### named Ably. Ably mimics pubsub Streaming concept,     ####
#### which might be extremely useful for any start-ups.    ####
####                                                       ####
###############################################################

from ably import AblyRest
import logging
import json

from random import seed
from random import random

import json
import math
import random

from clsConfig import clsConfig as cf

# Global Section

logger = logging.getLogger('ably')
logger.addHandler(logging.StreamHandler())

ably_id = str(cf.conf['ABLY_ID'])

ably = AblyRest(ably_id)
channel = ably.channels.get('sd_channel')

# End Of Global Section

class clsPublishStream:
    def __init__(self):
        self.fnc = cf.conf['FNC']

    def pushEvents(self, srcDF, debugInd, varVa, flg):
        try:
            # JSON data
            # This is the default data for all the identified category
            # we've prepared. You can extract this dynamically. Or, By
            # default you can set their base trade details.

            json_data = [{'Year_Mon': '201911', 'Brazil': 0.0, 'Canada': 0.0, 'Germany': 0.0, 'India': 0.0, 'Indonesia': 0.0, 'UnitedKingdom': 0.0, 'UnitedStates': 0.0, 'Status': flg},
            {'Year_Mon': '201912', 'Brazil': 0.0, 'Canada': 0.0, 'Germany': 0.0, 'India': 0.0, 'Indonesia': 0.0, 'UnitedKingdom': 0.0, 'UnitedStates': 0.0, 'Status': flg}]

            jdata = json.dumps(json_data)

            # Publish a message to the sd_channel channel
            channel.publish('event', jdata)

            # Capturing the inbound dataframe
            iDF = srcDF

            # Adding new selected points
            covid_dict = iDF.to_dict('records')
            jdata_fin = json.dumps(covid_dict)

            # Publish rest of the messages to the sd_channel channel
            channel.publish('event', jdata_fin)

            jdata_fin = ''

            return 0

        except Exception as e:

            x = str(e)
            print(x)

            logging.info(x)

            return 1
