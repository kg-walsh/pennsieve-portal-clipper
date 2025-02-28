#%%
from ieeg.auth import Session
from dotenv import load_dotenv
import os
import pandas as pd
import re
from IPython import embed
from typing import Tuple, Dict, Optional
import numpy as np
from ieeg_clips.manualvalidation_data import ManualValidation
from ieeg_clips.get_ieegmetadata import IEEGmetadata

#%%
class IEEGClips(ManualValidation, IEEGmetadata):
    """A class to process IEEG datasets and generate clips."""
    
    def __init__(self):
        """Initialize the IEEGDatasetProcessor class."""
        super().__init__()  # Initialize parent class

    def get_manualvalidation_annotations(self, dataset_name: str) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
        """Retrieve manual validation annotations."""
        # Get HUP ID from dataset name
        hup_id = re.search(r'HUP(\d+)', dataset_name).group(1)
        record_id = self._get_record_id(hup_id)
        
        # Get seizure times and start times using parent class methods
        seizure_times = self.get_seizure_times([record_id])
        start_times = self.get_start_times([record_id])
        
        # Filter seizure times for this specific dataset
        seizure_times = seizure_times[seizure_times['IEEGname'] == dataset_name]
        
        # Determine the correct column based on dataset name pattern
        try:
            if dataset_name.endswith(('phaseII', 'phaseII_D01')):
                column_name = 'Unnamed: 2'
            elif dataset_name.endswith('phaseII_D02'):
                column_name = 'Unnamed: 3'
            elif dataset_name.endswith('phaseII_D03'):
                column_name = 'Unnamed: 4'
            else:
                day_match = re.search(r'phaseII_D(\d+)$', dataset_name)
                if day_match:
                    day_num = int(day_match.group(1))
                    column_name = f'Unnamed: {day_num + 1}'
                else:
                    column_name = 'Unnamed: 2'

            if column_name not in start_times.columns:
                raise KeyError(f"Column {column_name} not found in start_times DataFrame")

            actual_start_time = start_times[column_name]
            
            if actual_start_time.empty:
                raise ValueError(f"No start time found for record ID: {record_id}")

            return seizure_times, actual_start_time

        except Exception as e:
            print(f"Error processing dataset {dataset_name}: {str(e)}")
            return seizure_times, None

    def process_manual_validation_data(
        self,
        channels_df: pd.DataFrame,
        annotations_df: pd.DataFrame,
        metadata_dict: Dict,
        seizure_times: pd.DataFrame,
        actual_start_time: pd.Series
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """Process manual validation data and add it to metadata.
        
        Args:
            channels_df (pd.DataFrame): DataFrame containing channel information
            annotations_df (pd.DataFrame): DataFrame containing annotation information
            metadata_dict (Dict): Dictionary containing general metadata
            seizure_times (pd.DataFrame): DataFrame containing seizure timing information
            actual_start_time (pd.Series): Start time from manual validation
        
        Returns:
            Tuple containing:
                pd.DataFrame: Updated channels_df
                pd.DataFrame: Updated annotations_df with manual validation entries
                Dict: Updated metadata_dict including actual_start_time
        """
        # Add actual_start_time to metadata_dict
        metadata_dict['actual_start_time'] = actual_start_time.iloc[0]
        
        # Create new rows for annotations_df from seizure_times
        new_annotations = {
            'layer': ['manual_validation'] * len(seizure_times),
            'annotator': seizure_times['source'].tolist(),
            'description': ['seizure'] * len(seizure_times),
            'type': ['seizure'] * len(seizure_times),
            'start_time_usec': (seizure_times['start'] * 1e6).astype(int).tolist(),
            'end_time_usec': (seizure_times['end'] * 1e6).astype(int).tolist()
        }
        
        # Convert new annotations to DataFrame and append
        new_annotations_df = pd.DataFrame(new_annotations)
        annotations_df = pd.concat([annotations_df, new_annotations_df], ignore_index=True)

        # Sort annotations_df by start_time_usec
        annotations_df = annotations_df.sort_values(by='start_time_usec')
        
        return channels_df, annotations_df, metadata_dict

    def find_dataset_clips(self, dataset_name: str, annotations_df: pd.DataFrame, metadata_dict: Dict) -> pd.DataFrame:
        """Create a DataFrame of 1-minute clips with annotation information.
        
        Args:
            dataset_name (str): Name of the dataset
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
        
        # Create base DataFrame
        clips_df = pd.DataFrame({
            'start_time_usec': start_times_usec,
            'end_time_usec': end_times_usec,
            'has_events': 0,
            'events': '',
            'annotators': '',
            'layers': '',
            'is_interictal': True  # Initialize all clips as interictal
        })
        
        # Convert 2 hours to microseconds
        two_hours_usec = int(2 * 60 * 60 * 1e6)

        
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
                clips_df.at[idx, 'has_events'] = 1
                # Convert to strings before joining
                clips_df.at[idx, 'events'] = ', '.join(str(x) for x in overlaps['description'].unique())
                clips_df.at[idx, 'annotators'] = ', '.join(str(x) for x in overlaps['annotator'].unique())
                clips_df.at[idx, 'layers'] = ', '.join(str(x) for x in overlaps['layer'].unique())
                
                # Mark clips within 2 hours of this event as not interictal
                nearby_clips = (
                    (clips_df['start_time_usec'] >= clip_start - two_hours_usec) &
                    (clips_df['end_time_usec'] <= clip_end + two_hours_usec)
                )
                clips_df.loc[nearby_clips, 'is_interictal'] = False
        
        # Convert actual_start_time string to datetime
        actual_start_time = pd.to_datetime(metadata_dict['actual_start_time'])
        
        # Create timestamp index
        timestamps = []
        is_night = []  # New list to store night/day information
        for t in start_times_sec:
            days_elapsed = int(t // (24 * 3600))
            current_time = actual_start_time + pd.Timedelta(seconds=int(t))
            
            # Format for index
            day_str = f"Day {days_elapsed + 1} {current_time.strftime('%H:%M:%S')}"
            timestamps.append(day_str)
            
            # Check if current time is during night hours (19:00-08:00)
            hour = current_time.hour
            is_night.append(hour >= 19 or hour < 8)
        
        clips_df.index = pd.Index(timestamps, name='timestamp')
        clips_df['is_night'] = is_night  # Add the new column

        # Interictal clips are those that are not night and not from Day 1
        interictal_clips = clips_df[~(clips_df['is_night'] | clips_df.index.str.contains('Day 1')) 
                                    & clips_df['is_interictal']]
        
        # make a data folder in the current directory with the name of the dataset
        os.makedirs(f'data/{dataset_name}', exist_ok=True)

        # save the clips_df and interictal_clips to csv
        clips_df.to_csv(f'data/{dataset_name}/all_clips.csv')
        interictal_clips.to_csv(f'data/{dataset_name}/interictal_clips.csv')
        annotations_df.to_csv(f'data/{dataset_name}/all_annotations.csv', index=False)
        
        # Save metadata_dict as readable text file
        with open(f'data/{dataset_name}/metadata.txt', 'w') as f:
            for key, value in metadata_dict.items():
                f.write(f'{key}: {value}\n')

        return clips_df, interictal_clips

# %%
if __name__ == '__main__':

    subjects_to_find = [
        'sub-RID0222', 'sub-RID0412', 'sub-RID0595', 'sub-RID0621', 'sub-RID0675',
        'sub-RID0679', 'sub-RID0700', 'sub-RID0785', 'sub-RID0796', 'sub-RID0852',
        'sub-RID0883', 'sub-RID0893', 'sub-RID0941', 'sub-RID0967'
    ]
     
    # Create processor instance
    processor = IEEGDatasetProcessor()

    
    # Get manual validation annotations
    seizure_times, start_times = processor.get_manualvalidation_annotations(dataset_name)
        
    # Get dataset metadata from IEEG
    channels_df, annotations_df, metadata_dict = processor.get_dataset_metadata(dataset_name)
        
    # Process manual validation data
    channels_df, annotations_df, metadata_dict = processor.process_manual_validation_data(
            channels_df, annotations_df, metadata_dict, seizure_times, start_times
        )
        
    # Find dataset clips
    clips_df, interictal_clips = processor.find_dataset_clips(
                dataset_name, annotations_df, metadata_dict
        )
    
# %%