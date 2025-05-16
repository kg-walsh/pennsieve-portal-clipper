import pandas as pd
import os
from redcap_data import Redcap
from IPython import embed

#%%

class ManualValidation(Redcap):
    """A class to handle manual validation annotations from Google Sheets."""
    
    def __init__(self):
        """Initialize the ManualValidation class."""
        super().__init__()  # Initialize parent Redcap class
        self.sheet_id = os.getenv('SHEET_ID_MANUAL_VALIDATION')
        self.manualvalidation_url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/"
        self.manual_validation_csv = os.getenv('SHEET_ID_LOCATION')
        self.identifiers = os.getenv('IDENTIFIERS')

    # def _get_record_id(self, hup_id: str) -> str:
    #     """Get the record ID from the name."""

    #     df_redcap = self.get_redcap_data()
    #     df_redcap = df_redcap.reset_index(names=['record_id']).set_index('hupsubjno').reindex(hup_id).reset_index()

    #     record_id = df_redcap['record_id']

    #     return record_id

    def get_actual_start_times(self, record_id: list[str] = None) -> pd.DataFrame:
        return pd.DataFrame()
    #     """Retrieve start times from Google Sheets.
        
    #     Args:
    #         record_id (list[str]): List of record IDs of the subjects to process
    #     """
    #     sheet_name = os.getenv('SHEET_NAME_MANUAL_VALIDATION_START_TIME')
    #     start_times = f"{self.manualvalidation_url}gviz/tq?tqx=out:csv&sheet={sheet_name}"
    #     start_times = pd.read_csv(start_times)
    #     start_times_hup = start_times[start_times['name'].astype(str).str.startswith('HUP')]
    #     start_times_hup.loc[:, 'name'] = start_times_hup.loc[:, 'name'].str.replace('HUP', '')

    #     start_times_hup.index = self._get_record_id(start_times_hup.name)

    #     if record_id is not None:
    #         start_times_hup = start_times_hup[start_times_hup.index.isin(record_id)] 

    #     return start_times_hup

    

    def get_hup_id(self, seizure_times: list[str] = None) -> pd.DataFrame:
        """ Map HUP IDs to R61 IDs."""
        ids = pd.read_csv(self.identifiers)
        id_map = dict(zip(ids['HUP_ID'], ids['R61_ID']))
        seizure_times['Patient'] = seizure_times['Patient'].map(id_map)

        return seizure_times

    def get_seizure_times(self, record_id: list[str] = None) -> pd.DataFrame:
        """Check if the record ID has seizure times.
        
        Args:
            record_id (list[str]): List of record IDs of the subjects to process
        
        Returns:
            pd.DataFrame: DataFrame containing the seizure times
        """
        #sheet_name = os.getenv('SHEET_NAME_MANUAL_VALIDATION_SEIZURE_TIME')
        #seizure_times_url = f"{self.manualvalidation_url}gviz/tq?tqx=out:csv&sheet={sheet_name}"
        seizure_times = pd.read_csv(self.manual_validation_csv)
        print(seizure_times)

        seizure_times = self.get_hup_id(seizure_times)
        print(seizure_times)
        seizure_times = seizure_times[seizure_times['Patient'].isin(record_id)]
        print(seizure_times)


        return seizure_times
    

# %%

if __name__ == '__main__':

    subjects_to_find = [
        "sub-PENN001"]

    validated_data = ManualValidation()
    #ieegportal_data_df = validated_data.get_redcap_data(subjects=subjects_to_find)
    #ieegportal_data_df = validated_data.expand_ieeg_days_rows(ieegportal_data_df)

    #start_times_df = validated_data.get_actual_start_times(record_id=subjects_to_find)
    seizure_times_df = validated_data.get_seizure_times(record_id=subjects_to_find)
    print(seizure_times_df)

    # Find missing records
    #missing_start_times = ieegportal_data_df[~ieegportal_data_df.index.isin(start_times_df.index)].index.unique()
    #missing_seizure_times = ieegportal_data_df[~ieegportal_data_df.index.isin(seizure_times_df.index)].index.unique()

    # # Create a mapping of record_id to hupsubjno
    # id_mapping = ieegportal_data_df['hupsubjno'].to_dict()

    # # Print missing records in a formatted way
    # print("\nMissing Records Summary:")
    # print("-" * 50)
    
    # print("Records missing start times:")
    # if len(missing_start_times) > 0:
    #     for record in missing_start_times:
    #         hup_id = id_mapping.get(record, 'Unknown')
    #         print(f"  • {record} (HUP{hup_id})")
    # else:
    #     print("  None")
    
    # print("\nRecords missing seizure times:")
    # if len(missing_seizure_times) > 0:
    #     for record in missing_seizure_times:
    #         hup_id = id_mapping.get(record, 'Unknown')
    #         print(f"  • {record} (HUP{hup_id})")
    # else:
    #     print("  None")
    
    # print("-" * 50)

# %%
