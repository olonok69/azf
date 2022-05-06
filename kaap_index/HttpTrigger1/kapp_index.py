import numpy as np
import pandas as pd
import json


class kapp_index(object):

    def __init__(self,
                 file,
                 country,
                 MS_Threshold,
                 multiplier=325,
                 additive=450,
                 RP_alpha=-1,
                 UC_alpa=0):

        self.file=file
        self.datan=None
        self.country=country.upper()
        self.multiplier=multiplier
        self.additive=additive
        self.MS_Threshold=MS_Threshold #If MS < 105, then use (PTA - µ)/α * Weight, where µ = 105, else use 0
        self.RP_alpha=RP_alpha # Use -1*ABS((PTA - µ))/α * Weight, where µ = -1
        self.UC_alpa=UC_alpa # UUse -1*ABS((PTA - µ))/α * Weight, where µ = 0
        self.vdataMS=[]
        self.vdataRP=[]
        self.vdataUC=[]
        self.vdataRV=[] # only UK
        self.vdataRTPRV=[]# only JE
        self.vdataRTPSV=[]# only JE
        self.json_stats=None
        self.weigth=False

        assert (self.country).upper() in ['US', 'JE', 'UK']
        assert (self.MS_Threshold == 105 and (self.country).upper() in ['US','JE']) or \
               (self.MS_Threshold == 1.18 and (self.country).upper() == 'UK')


        return

    def clean_data(self, data):
        """
        read Json File with all statistics
        :param path:
        :return:
        """
        data=data[data.Weight != "n/a"]
        data.dropna(subset=['Mean', 'StDev', 'Weight','sample'], how='any', inplace=True)
        return data

    def read_stat_json(self, path):
        """
        read json from file system
        :param path:
        :return:
        """

        with open(path, 'r') as fp:
            stats = json.load(fp)

        self.json_stats=stats

        return
    def read_traits(self):
        """
        prepare Trait file for use
        :return:
        """
        data= pd.read_csv(self.file)
        if 'Weight' in data.columns:
            data['Mean'], data['StDev']  = np.nan, np.nan
            self.weigth=True
        else:
            self.weigth=False
            data['Mean'], data['StDev'], data['Weight'] = np.nan, np.nan, np.nan

        return data
    
    def read_traits_json(self, payload):
        """
        read the trait sample from request JSON payload key Trait
        convert to dataframe and prepare it for use
        :return:
        """
        data= pd.read_json(payload['Trait'])
        if 'Weight' in data.columns:
            data['Mean'], data['StDev']  = np.nan, np.nan
            self.weigth=True
        else:
            self.weigth=False
            data['Mean'], data['StDev'], data['Weight'] = np.nan, np.nan, np.nan

        return data

    def data_from_stats(self, data):
        """
        Prepare data after reading file from filesystem
        :param data:
        :return:
        """

        data = kapp_index.create_dataframe(data, self.country, self.json_stats, self.weigth)

        data = self.clean_data(data)
        data = data[['Trait', 'Mean', 'StDev', 'Weight', 'sample']]
        data = self.prepare_matrix(data)

        return data

    def load_from_csv(self):
        """
        Read examples from csv

        :return:
        """

        data = pd.read_csv(self.file)


        data.columns = ['Trait', 'Mean', 'StDev', 'Weight', 'Calc', 'sample']
        data = self.clean_data(data)
        data = data[['Trait', 'Mean', 'StDev', 'Weight',  'sample']]

        data= self.prepare_matrix(data)

        return

    def kapp_calculation(self):
        """
        Calculate Kaap Index

        first calculate sum of all traits. all cases normal cases follow the rule (Trait - mean / std) * weigth
        second special cases depending of the country for vm + rp + uc + rv + rtpsv + rtprv
        third sum_trait = sum_trait + vm + rp + uc + rv + rtpsv + rtprv
        four uk and je divide the sum of traits by 100
        fith apply (sum_trait * multiplier) + additive

        :return: kapp index
        """
        # sum all trait except especials
        sum_trait = np.sum((self.datan[:, 3] - self.datan[:, 0]) / self.datan[:, 1] * self.datan[:, 2])

        # calculate VM if exists
        if len(self.vdataMS)>0 and self.country in ['US','JE']:
            if self.vdataMS[3] < self.MS_Threshold:
                vm = (self.vdataMS[3] - self.MS_Threshold) / self.vdataMS[1] * self.vdataMS[2]
            else:
                vm = 0
        elif len(self.vdataMS)>0 and self.country=='UK':
            if self.vdataMS[3] < self.MS_Threshold:
                vm = (self.vdataMS[3] - self.MS_Threshold) / self.vdataMS[1] * self.vdataMS[2]
            else:
                vm=0
        else:
            vm=0
        # calculate RP if exists
        if len(self.vdataRP) >0 and self.country in ['US', 'UK'] :
            rp = (-1*abs(self.vdataRP[3]- self.RP_alpha))/self.vdataRP[1]*self.vdataRP[2]
        else:
            rp=0

        # calculate UC if exists
        if len(self.vdataUC) > 0 and self.country in ['US','UK', 'JE']:
            uc = (-1 * abs(self.vdataUC[3] - self.UC_alpa)) / self.vdataUC[1] * self.vdataUC[2]
        else:
            uc = 0

        # calculate RV if exists
        if len(self.vdataRV)>0 and self.country in ['UK'] :
            rv = (-1 * abs(self.vdataRV[3] - self.RP_alpha)) / (self.vdataRV[1] * self.vdataRV[2])
        else:
            rv=0

        # calculate RTP-SV if exists
        if len(self.vdataRTPSV) > 0 and self.country in [ 'JE']:
            rtpsv = (-1 * abs(self.vdataRTPSV[3] - self.UC_alpa)) / self.vdataRTPSV[1] * self.vdataRTPSV[2]
        else:
            rtpsv = 0

        # calculate RTP-RV if exists
        if len(self.vdataRTPRV) > 0 and self.country in [ 'JE']:
            rtprv = (-1*abs(self.vdataRTPRV[3]- self.RP_alpha))/self.vdataRTPRV[1]*self.vdataRTPRV[2]
        else:
            rtprv = 0

        # calculate sum of traits
        sum_trait = sum_trait + vm + rp + uc + rv + rtpsv + rtprv

        # if UK or JE divide by 100 sum of all traits calculations
        if self.country=='UK' or self.country=='JE':
            sum_trait=sum_trait/100

        # Calculate Kapp Index
        kapp = (sum_trait * self.multiplier) + self.additive

        return kapp


    def prepare_matrix(self, data):
        """
        prepare a numpy matrix with the relevant columns for normal traits and vectors for special cases
        vm , rp , uc , rv , rtpsv , rtprv
        remove the special cases from the matrix

        :param data: dataframe with a trait and its statistics
        :return:
        """

        if len(data[data.Trait == 'MS']) == 1:
            dataMS = data[data.Trait == 'MS']
            self.vdataMS = dataMS[['Mean', 'StDev', 'Weight', 'sample']].values[0]
        if len(data[data.Trait == 'UC']) == 1:
            dataUC = data[data.Trait == 'UC']
            self.vdataUC = dataUC[['Mean', 'StDev', 'Weight', 'sample']].values[0]
        if len(data[data.Trait == 'RP']) == 1:
            dataRP = data[data.Trait == 'RP']
            self.vdataRP = dataRP[['Mean', 'StDev', 'Weight', 'sample']].values[0]

        if len(data[data.Trait == 'RTP-RV']) == 1 and self.country == "JE":
            dataRTPRV = data[data.Trait == 'RTP-RV']
            self.vdataRTPRV = dataRTPRV[['Mean', 'StDev', 'Weight', 'sample']].values[0]

        if len(data[data.Trait == 'RTP-SV']) == 1 and self.country == "JE":
            dataRTPSV = data[data.Trait == 'RTP-SV']
            self.vdataRTPSV = dataRTPSV[['Mean', 'StDev', 'Weight', 'sample']].values[0]

            # RV special case only UK. May come alone or together with RP
        if (len(data[data.Trait == 'RV']) == 1 or len(data[data.Trait == 'RP-RV']) == 1) and self.country == "UK":
            if len(data[data.Trait == 'RV']) == 1:
                dataRV = data[data.Trait == 'RV']
                self.vdataRV = dataRV[['Mean', 'StDev', 'Weight', 'sample']].values[0]
            elif len(data[data.Trait == 'RP-RV']) == 1:
                dataRV = data[data.Trait == 'RP-RV']
                self.vdataRV = dataRV[['Mean', 'StDev', 'Weight', 'sample']].values[0]

        data = data[data.Trait != "MS"]
        data = data[data.Trait != "UC"]
        # Only US
        if self.country == "US":
            data = data[data.Trait != "RP"]
        # Only UK
        if self.country == "UK":
            data = data[data.Trait != "RV"]
            data = data[data.Trait != "RP-RV"]
        if self.country == 'JE':
            data = data[data.Trait != 'RTP-SV']
            data = data[data.Trait != 'RTP-RV']

        self.datan = data[['Mean', 'StDev', 'Weight', 'sample']].values

        data= data[['Trait','Mean', 'StDev', 'Weight', 'sample']]

        return data

    @staticmethod
    def fill_stats(row, stats, country, key):
        """
        select statistic for a country

        :param row:
        :param stats:
        :param country:
        :param key:
        :return:
        """
        def search(name, dicc):
            return list(filter(lambda stat: stat['Trait'] == name, dicc))

        # subset of statistics for this country
        statuss = stats[country]

        trait_stats = search(row['Trait'], statuss)
        if len(trait_stats) == 1:
            return trait_stats[0][key]
        else:
            return np.nan

    @staticmethod
    def create_dataframe(data, country, stats, weights=False):
        """
        fill statistics for a kaap index sample. This use the dictionary with all statistics and apply the  method
        fill_stats to populate mean and std
        case that the trait sample does not contains weigths apply default weigth conatained in all_stats.json
        :param data:
        :param country:
        :param stats:
        :return:
        """

        data['Mean'] = data.apply(lambda row: kapp_index.fill_stats(row, stats, country, "Mean"), axis=1)
        data['StDev'] = data.apply(lambda row: kapp_index.fill_stats(row, stats, country, "StDev"), axis=1)
        if weights==False:
            data['Weight'] = data.apply(lambda row: kapp_index.fill_stats(row, stats, country, "Weight"), axis=1)

        return data