# %%
import pandas as pd
import requests
from io import StringIO
from dotenv import load_dotenv, find_dotenv
import os

# %%
class Redcap:
    """
    A class to handle REDCap data retrieval and processing.
    """
    def __init__(self, token: str = None, report_id: str = None):
        """
        Initialize the Redcap client.
        
        Args:
            token (str, optional): REDCap API token. If None, loads from environment variables.
            report_id (str, optional): REDCap report ID. If None, loads from environment variables.
        """
        env_path = find_dotenv()
        print(f"Found .env file at: {env_path}")
        load_dotenv()
        self.token = token if token else os.getenv('REDCAP_TOKEN')
        self.report_id = report_id if report_id else os.getenv('REDCAP_REPORT_ID')
        self.redcap_url = 'https://redcap.med.upenn.edu/api/'

    def expand_ieeg_days_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expands rows where ieegportalsubjno contains a D-number range (e.g., D04-D07).
        Each range is expanded into individual rows with sequential D-numbers.
        
        Args:
            df (pd.DataFrame): Input DataFrame with potential D-number ranges
            
        Returns:
            pd.DataFrame: DataFrame with expanded rows for D-number ranges
        """
        expanded_rows = []
        for idx, row in df.iterrows():
            if isinstance(row['ieegportalsubjno'], str) and '-D' in row['ieegportalsubjno']:
                base_name, d_range = row['ieegportalsubjno'].rsplit('_', 1)
                start_d, end_d = d_range.split('-')
                start_num = int(start_d.replace('D', ''))
                end_num = int(end_d.replace('D', ''))
                
                for d_num in range(start_num, end_num + 1):
                    new_row = row.copy()
                    new_row['ieegportalsubjno'] = f"{base_name}_D{d_num:02d}"
                    expanded_rows.append((idx, new_row))
            else:
                expanded_rows.append((idx, row))
        
        return pd.DataFrame([row for _, row in expanded_rows], index=[idx for idx, _ in expanded_rows])

    def get_redcap_data(self, report_id: str = None, subjects: list = None) -> pd.DataFrame:
        """
        Fetches data from REDCap and returns it as a pandas DataFrame.
            
        Args:
            report_id (str, optional): REDCap report ID to fetch. 
                                     If None, uses the ID from initialization.
            subjects (list, optional): List of RIDs to filter for (e.g., ['RID0001', 'RID0002']).
                                     If None, returns all subjects.
        
        Returns:
            pd.DataFrame: DataFrame containing the REDCap data, filtered for specified subjects
        """
        data = {
            'token': self.token,
            'content': 'report',
            'format': 'csv',
            'report_id': report_id if report_id else self.report_id,
            'csvDelimiter': '',
            'rawOrLabel': 'label',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'returnFormat': 'csv'
        }

        print('Fetching data from REDCap...')
        response = requests.post(self.redcap_url, data=data)
        print('HTTP Status: ' + str(response.status_code))
        if response.status_code == 200:
            print('Data fetched successfully.')
        df = pd.read_csv(StringIO(response.text))
        df['record_id'] = 'sub-RID' + df['record_id'].astype(str).str.zfill(4)
        df = df.set_index('record_id').sort_index()
        
        if subjects:
            subjects = ['sub-' + s if not s.startswith('sub-') else s for s in subjects]
            df = df[df.index.isin(subjects)]
        
        return df

# %%

if __name__ == '__main__':

    subjects_to_find = ['sub-RID0015', 'sub-RID0030', 'sub-RID0063', 'sub-RID0066', 'sub-RID0595',]

    redcap = Redcap()  # Initialize the Redcap client
    df_redcap = redcap.get_redcap_data(subjects=subjects_to_find)  # Get the data
    print(df_redcap)
    f_ieegportal = redcap.expand_ieeg_days_rows(df_redcap) # Expand IEEG rows
    #print(f_ieegportal)

# %%
