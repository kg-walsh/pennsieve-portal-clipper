#%%
from ieeg.auth import Session
import os
import pandas as pd
from typing import Tuple, Dict
from redcap_data import Redcap
from pathlib import Path

#%%
class IEEGmetadata(Redcap):

    def __init__(self):
        super().__init__()
        self.session = self.setup_ieeg_session()

    def setup_ieeg_session(self) -> Session:
        """Set up and return an IEEG session using environment variables."""
        ieeg_user = os.getenv('IEEG_USERNAME')
        ieeg_password = os.getenv('IEEG_PASSWORD')
        
        if not ieeg_user or not ieeg_password:
            raise ValueError("IEEG credentials not found in environment variables")
        
        print(f'Logging into IEEG: {ieeg_user} / ****')
        return Session(ieeg_user, ieeg_password)
    
    def get_dataset_metadata(self, dataset_name: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """Get dataset metadata from IEEG."""
        ds = self.session.open_dataset(dataset_name)
        
        channel_labels = ds.get_channel_labels()
        channel_indices = ds.get_channel_indices(channel_labels)
        sampling_rate = ds.get_time_series_details(channel_labels[0]).sample_rate

        start_time_usec = ds.start_time
        end_time_usec = ds.end_time
        duration_sec = (ds.end_time - ds.start_time)/1e6

        # Get one second of data
        # one_secIEEG = ds.get_dataframe(ds.start_time, 1000000, ds.get_channel_indices(ds.get_channel_labels()))

        # Get all annotations
        all_annotations = ds.get_annotation_layers()
        annotation_layers = list(all_annotations.keys())

        # Initialize lists to store annotation data
        annotations_data = {
            'layer': [],
            'annotator': [],
            'description': [],
            'type': [],
            'start_time_usec': [],
            'end_time_usec': []
        }

        # get all events
        for layer in iter(annotation_layers):
            events = ds.get_annotations(layer)
            for event in iter(events):
                # Append each annotation's data to the respective lists
                annotations_data['layer'].append(event.layer)
                annotations_data['annotator'].append(event.annotator)
                annotations_data['description'].append(event.description)
                annotations_data['type'].append(event.type)
                annotations_data['start_time_usec'].append(event.start_time_offset_usec)
                annotations_data['end_time_usec'].append(event.end_time_offset_usec)
        
        # Create DataFrame from the collected data
        annotations_df = pd.DataFrame(annotations_data)

        # Create channels DataFrame
        channels_df = pd.DataFrame({
            'label': channel_labels,
            'index': channel_indices
        })

        # make a dictionary for general metadata
        metadata_dict = {
            'sampling_rate': sampling_rate,
            'start_time_usec': start_time_usec,
            'end_time_usec': end_time_usec,
            'duration_sec': duration_sec,
        }

        self.session.close_dataset(ds)

        return channels_df, annotations_df, metadata_dict
    
    def save_metadata(self, record_id, dataset_name, path_to_save: Path = Path(__file__).parent.parent / 'data'):
        """Save the metadata to a file.
        
        Args:
            record_id: The ID of the record
            dataset_name: Name of the dataset
            path_to_save: Path where metadata will be saved. Defaults to 'data'
        """
        Path(path_to_save / record_id / dataset_name).mkdir(parents=True, exist_ok=True)

        channels_df, annotations_df, metadata_dict = self.get_dataset_metadata(dataset_name)

        channels_df.to_csv(Path(path_to_save) / record_id / dataset_name / 'channels.csv', index=False)
        annotations_df.to_csv(Path(path_to_save) / record_id / dataset_name / 'annotations.csv', index=False)
        with open(Path(path_to_save) / record_id / dataset_name / 'metadata.txt', 'w') as f:
            for key, value in metadata_dict.items():
                f.write(f"{key}: {value}\n")
    
# %%
if __name__ == '__main__':

    subjects_to_find = [
        'sub-RID0222', 'sub-RID0412', 'sub-RID0595', 'sub-RID0621', 'sub-RID0675',
        'sub-RID0679', 'sub-RID0700', 'sub-RID0785', 'sub-RID0796', 'sub-RID0852',
        'sub-RID0883', 'sub-RID0893', 'sub-RID0941', 'sub-RID0967'
    ]

    ieeg = IEEGmetadata()
    ieeg_data_df = ieeg.get_redcap_data(subjects=subjects_to_find)
    ieeg_data_df = ieeg.expand_ieeg_days_rows(ieeg_data_df)

    for record_id, data in ieeg_data_df.iterrows():
        dataset_name = data['ieegportalsubjno']
        ieeg.save_metadata(record_id=record_id, dataset_name=dataset_name)

# %%
