#%%
from ieeg.auth import Session
import os
import numpy as np
import pandas as pd
from typing import Tuple, Dict
from redcap_data import Redcap
from pathlib import Path
from IPython import embed
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
        
        print(f'Logging into IEEG Portal: {ieeg_user} / ****')
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

        clips_df = self._ieeg_clips(annotations_df, metadata_dict)

        return channels_df, annotations_df, metadata_dict, clips_df
    
    def _ieeg_clips(self, annotations_df: pd.DataFrame, metadata_dict: Dict) -> pd.DataFrame:
        """Create a DataFrame of 1-minute clips with annotation information.
        
        Args:
            annotations_df (pd.DataFrame): DataFrame containing annotations
            metadata_dict (Dict): Dictionary containing metadata including duration and actual_start_time
        
        Returns:
            pd.DataFrame: Clips DataFrame with columns for start_time, end_time, has_events,
                         events, annotators, layers, actual_time, is_night, and is_interictal
        """
        # Calculate number of 1-minute clips
        total_minutes = int(metadata_dict['duration_sec'] / 60)
        
        # Create arrays for start and end times in seconds
        start_times_sec = np.arange(total_minutes) * 60
        end_times_sec = start_times_sec + 60
        
        # Convert to microseconds and ensure integer type
        start_times_usec = (start_times_sec * 1e6).astype(int)
        end_times_usec = (end_times_sec * 1e6).astype(int)
        
        # Create base DataFrame with consistent boolean type for has_events
        clips_df = pd.DataFrame({
            'start_time_usec': start_times_usec,
            'end_time_usec': end_times_usec,
            'has_events': False,
            'events': '',
            'annotators': '',
            'layers': '',
            'close_to_event': False  # Initialize all clips as not being close to events
        })
        
        # Check for overlaps and update clips DataFrame
        clips_df = self._check_clip_overlaps(clips_df, annotations_df)
        
        return clips_df
    
    def _check_clip_overlaps(self, clips_df: pd.DataFrame, annotations_df: pd.DataFrame, 
                            hours_window: float = 2.0) -> pd.DataFrame:
        """Check for overlaps between clips and annotations.
        
        Args:
            clips_df (pd.DataFrame): DataFrame containing clip information
            annotations_df (pd.DataFrame): DataFrame containing annotations
            hours_window (float): Hours before and after an event to mark as close. Defaults to 2.0
        
        Returns:
            pd.DataFrame: Updated clips DataFrame with overlap information
        """
        # Convert hours to microseconds
        hours_window_usec = int(hours_window * 60 * 60 * 1e6)
        
        
        # Check for overlaps with annotations
        for idx, clip in clips_df.iterrows():
            clip_start = clip['start_time_usec']
            clip_end = clip['end_time_usec']

            # Find overlapping annotations
            overlaps = annotations_df[
                ((annotations_df['start_time_usec'] >= clip_start) & (annotations_df['start_time_usec'] < clip_end)) |
                ((annotations_df['end_time_usec'] > clip_start) & (annotations_df['end_time_usec'] <= clip_end)) |
                ((annotations_df['start_time_usec'] <= clip_start) & (annotations_df['end_time_usec'] >= clip_end))
            ]
            
            if not overlaps.empty:
                clips_df.at[idx, 'has_events'] = True
                # Convert to strings before joining
                clips_df.at[idx, 'events'] = ', '.join(str(x) for x in overlaps['description'].unique())
                clips_df.at[idx, 'annotators'] = ', '.join(str(x) for x in overlaps['annotator'].unique())
                clips_df.at[idx, 'layers'] = ', '.join(str(x) for x in overlaps['layer'].unique())
                
                # Mark clips within specified hours of this event as being close to an event
                nearby_clips = (
                    (clips_df['start_time_usec'] >= clip_start - hours_window_usec) &
                    (clips_df['end_time_usec'] <= clip_end + hours_window_usec)
                )
                clips_df.loc[nearby_clips, 'close_to_event'] = True
        
        return clips_df

    def save_metadata(self, record_id, dataset_name, path_to_save: Path = Path(__file__).parent.parent / 'data'):
        """Save the metadata to a file.
        
        Args:
            record_id: The ID of the record
            dataset_name: Name of the dataset
            path_to_save: Path where metadata will be saved. Defaults to 'data'
        """
        Path(path_to_save / record_id / dataset_name).mkdir(parents=True, exist_ok=True)

        channels_df, annotations_df, metadata_dict, clips_df = self.get_dataset_metadata(dataset_name)

        channels_df.to_csv(Path(path_to_save) / record_id / dataset_name / 'channels.csv', index=False)
        annotations_df.to_csv(Path(path_to_save) / record_id / dataset_name / 'annotations.csv', index=False)
        clips_df.to_csv(Path(path_to_save) / record_id / dataset_name / 'clips.csv', index=False)
        with open(Path(path_to_save) / record_id / dataset_name / 'metadata.txt', 'w') as f:
            for key, value in metadata_dict.items():
                f.write(f"{key}: {value}\n")

        return channels_df, annotations_df, metadata_dict, clips_df
    
    def get_dataset_clips(self, dataset_name: str, start_time_usec: int, end_time_usec: int) -> Tuple[pd.DataFrame, float, list[str]]:
        """Get dataset metadata from IEEG."""
        ds = self.session.open_dataset(dataset_name)
       
        start_time_usec = start_time_usec
        end_time_usec = end_time_usec
        duration_usec = end_time_usec - start_time_usec

        channel_labels = ds.get_channel_labels()
        channel_indices = ds.get_channel_indices(channel_labels)
        ieeg_clip = ds.get_dataframe(start_time_usec, duration_usec, channel_indices)
        sampling_rate = ds.get_time_series_details(channel_labels[0]).sample_rate
        
        self.session.close_dataset(ds)

        return ieeg_clip, sampling_rate, channel_labels
    
# %%
if __name__ == '__main__':

    subjects_to_find = [
        'sub-RID0190'
    ]

    ieeg = IEEGmetadata()
    ieeg_data_df = ieeg.get_redcap_data(subjects=subjects_to_find)
    ieeg_data_df = ieeg.expand_ieeg_days_rows(ieeg_data_df)

    for record_id, data in ieeg_data_df.iterrows():
        dataset_name = data['ieegportalsubjno']
        ieeg.save_metadata(record_id=record_id, dataset_name=dataset_name)

# %%
