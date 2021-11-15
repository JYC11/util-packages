import json
from simple_salesforce import Salesforce
import pandas as pd
import gspread

class GoogleSheetConnector:
    def __init__(self):
        self.gc = gspread.oauth()
    
    def getData(self,url,sheetname,range_=None,frame=True):
        spreadsheet = self.gc.open_by_url(url)
        sheet = spreadsheet.worksheet(sheetname)
        if frame == True: #returns a dataframe
            if range_ == None:
                everything = pd.DataFrame(sheet.get_all_records())
                return everything
            else:
                everything_ = sheet.get(range_)
                columns = everything_[0]
                data = everything_[1:]
                everything = pd.DataFrame(data,columns=columns)
                return everything
        else: #returns an array of arrays
            if range_ == None:
                everything = sheet.get_all_values()
                return everything
            else:
                everything = sheet.get(range_)
                return everything

    def next_available_row(self,worksheet):
        str_list = list(filter(None, worksheet.col_values(1)))
        return str(len(str_list)+1)
    
    def writeData(self,data,target,writeOptions=[False,False]):
        url, sheet_name, range_ = target
        addToBottom, clearAll = writeOptions
        spreadsheet = self.gc.open_by_url(url)
        sheet = spreadsheet.worksheet(sheet_name)
        data.fillna('-', inplace=True)
        
        if addToBottom == True and not any(char.isdigit() for char in range_):
            range_ = range_+self.next_available_row(sheet)
        
        try:
            if clearAll == True:
                sheet.clear()
            if addToBottom == True:
                sheet.update(range_,data.values.tolist())
            else:
                sheet.update(range_,[data.columns.values.tolist()] + data.values.tolist())
        except Exception as e:
            print(e)

class DataConnectorForSalesforce(GoogleSheetConnector):
    def __init__(self):
        GoogleSheetConnector.__init__(self)
        self.sf = self.authorize()
        self.nums = [str(_) for _ in range(0,10)]
    
    def authorize(self):
        with open("credentials/salesforce.json") as f:
            credentials = json.load(f)
        userName = credentials['username']
        password = credentials['password']
        securityToken = credentials['security token']
        sf = Salesforce(username=userName, password=password, security_token=securityToken)
        return sf
    
    def queryData(self,query):
        try:
            query_result = self.sf.query_all(query)
        except Exception as e:
            print(e)
            return
        df = pd.DataFrame(query_result["records"]) #turn list of dicts into dataframe
        df.fillna('-', inplace=True)
        df = df.drop(columns=[df.columns[0]]) #drop first uncessary column
        return df
    
    def writeToGoogleSheetsDirect(self,query,target,writeOptions=[False,False]):
        df = self.queryData(query)     
        self.writeData(df,target,writeOptions)