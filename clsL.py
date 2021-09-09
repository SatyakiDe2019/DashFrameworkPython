#####################################################
#### Written By: SATYAKI DE                      ####
#### Written On: 26-Jul-2021                     ####
####                                             ####
#### Objective: This script is a log             ####
#### file, that is useful for debugging purpose. ####
####                                             ####
#####################################################

import pandas as p
import os
import platform as pl

class clsL(object):
    def __init__(self):
        self.path = os.path.dirname(os.path.realpath(__file__))

    def logr(self, Filename, Ind, df, subdir=None, write_mode='w', with_index='N'):
        try:
            x = p.DataFrame()
            x = df

            sd = subdir

            os_det = pl.system()

            if sd == None:
                if os_det == "windows":
                    fullFileName = self.path + '\\' + Filename
                else:
                    fullFileName = self.path + '/' + Filename
            else:
                if os_det == "windows":
                    fullFileName = self.path + '\\' + sd + '\\' + Filename
                else:
                    fullFileName = self.path + '/' + sd + '/' + Filename

            if (with_index == 'N'):
                if ((Ind == 'Y') & (write_mode == 'w')):
                    x.to_csv(fullFileName, index=False)
                else:
                    x.to_csv(fullFileName, index=False, mode=write_mode, header=None)
            else:
                if ((Ind == 'Y') & (write_mode == 'w')):
                    x.to_csv(fullFileName)
                else:
                    x.to_csv(fullFileName, mode=write_mode, header=None)

            return 0
        except Exception as e:
            y = str(e)
            print(y)

            return 3
