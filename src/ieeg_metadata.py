#%%
from ieeg.auth import Session
import os
import numpy as np
import pandas as pd
from typing import Tuple, Dict
from redcap_data import Redcap
from pathlib import Path
from IPython import embed
import mne
import re

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
    
    def get_available_sessions(self, data_path, subject_id):
            """Get available sessions for the given subject(s)"""
            # Convert string to list if needed
            if isinstance(subject_id, str):
                subject_id = [subject_id]
                
            parent_path = Path(data_path)
            
            # Get all patient folders and their sessions
            session_data = []
            for patient_folder in parent_path.iterdir():
                # Skip if not a directory or not in our subject list
                if not patient_folder.is_dir() or patient_folder.name not in subject_id:
                    continue
                    
                # Go through implant sessions
                for session_folder in patient_folder.iterdir():
                    if not session_folder.is_dir() or not session_folder.name.startswith('ses-implant'):
                        continue
                        
                    session_data.append({
                        'patient_id': patient_folder.name,
                        'session': session_folder.name
                    })
            
            # Create dataframe
            return pd.DataFrame(session_data)
    
    def is_real_edf(self, file_path, min_size_kb=100):
        """
        Check if an EDF file is real (fully downloaded) or just a reference
        
        Parameters:
        file_path: Path to the EDF file
        min_size_kb: Minimum size in KB to consider a real file (default: 100KB)
        
        Returns:
        bool: True if it's a real EDF file, False if it's likely just a reference
        """
        # Convert min_size to bytes
        min_size_bytes = min_size_kb * 1024
        
        # Get file size
        file_size = os.path.getsize(file_path)
        
        # Return True if file is larger than minimum size
        return file_size > min_size_bytes
    
    def get_available_edf_files(self, data_path, subject_id, session=None, only_pulled=False, min_size_kb=100):
        """
        Get available EDF files for the given subject(s) and optional session
        
        Parameters:
        data_path (str): Path to data directory
        subject_id (str or list): Subject ID(s) to find
        session (str, optional): If provided, only get EDFs from this specific session
        only_pulled (bool): If True, only include files that have been fully pulled (default: False)
        min_size_kb (int): Minimum size in KB to consider a file "pulled" (default: 100KB)
        
        Returns:
        pandas.DataFrame: DataFrame with EDF file information
        """
        # Get all sessions first (if session is None) or filter to the specific session
        if session is None:
            sessions_df = self.get_available_sessions(data_path, subject_id)
        else:
            # Create a DataFrame with just the specified session
            sessions_df = pd.DataFrame([{
                'patient_id': subject_id if isinstance(subject_id, str) else subject_id[0],
                'session': session
            }])
        
        # For each session, find EDF files
        edf_data = []
        for _, row in sessions_df.iterrows():
            patient_id = row['patient_id']
            session = row['session']
            
            # Build the ieeg path
            parent_path = Path(data_path)
            ieeg_folder = parent_path / patient_id / session / 'ieeg'
            
            if not ieeg_folder.exists() or not ieeg_folder.is_dir():
                # Add session without EDF files
                if not only_pulled:
                    edf_data.append({
                        'patient_id': patient_id,
                        'session': session,
                        'edf_file': None,
                        'edf_path': None,
                        'is_pulled': False
                    })
                continue
            
            # Find EDF files
            edf_files = list(ieeg_folder.glob('*.edf'))
            
            if not edf_files:
                # Add session without EDF files
                if not only_pulled:
                    edf_data.append({
                        'patient_id': patient_id,
                        'session': session,
                        'edf_file': None,
                        'edf_path': None,
                        'is_pulled': False
                    })
            else:
                # Add each EDF file
                for edf_file in edf_files:
                    # Check if the file is pulled
                    is_pulled = self.is_real_edf(edf_file, min_size_kb)
                    
                    # Only add if all files requested or the file is pulled
                    if not only_pulled or is_pulled:
                        edf_data.append({
                            'patient_id': patient_id,
                            'session': session,
                            'edf_file': edf_file.name,
                            'edf_path': str(edf_file),
                            'is_pulled': is_pulled
                        })
        
        return pd.DataFrame(edf_data)

    def get_dataset_metadata(self, edf_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict, pd.DataFrame]:
        """Get dataset metadata from an EDF file using MNE."""
        
        # Load the EDF file
        raw = mne.io.read_raw_edf(edf_path, preload=False)
        
        # Get channel information
        channel_labels = raw.ch_names
        channel_indices = list(range(len(channel_labels)))
        sampling_rate = raw.info['sfreq']
        
        # Get timing information
        start_time_usec = 0  # EDF files typically start at 0
        duration_sec = raw.times[-1]  # Duration in seconds
        end_time_usec = int(duration_sec * 1e6)  # Convert to microseconds
        
        # Create channels DataFrame
        channels_df = pd.DataFrame({
            'label': channel_labels,
            'index': channel_indices
        })
        
        # Get annotations (if any)
        annotations = raw.annotations
        
        # Initialize annotation data
        annotations_data = {
            'layer': [],
            'annotator': [],
            'description': [],
            'type': [],
            'start_time_usec': [],
            'end_time_usec': []
        }
        
        # Process annotations
        for i, annot in enumerate(annotations):
            # Convert MNE annotations to the same format as IEEG
            onset_sec = annot['onset']
            duration = annot['duration']
            description = annot['description']
            
            # Convert to microseconds
            start_time_usec = int(onset_sec * 1e6)
            end_time_usec = int((onset_sec + duration) * 1e6)
            
            # Add to annotations data
            annotations_data['layer'].append('MNE_Annotations')
            annotations_data['annotator'].append('MNE')
            annotations_data['description'].append(description)
            annotations_data['type'].append('Annotation')
            annotations_data['start_time_usec'].append(start_time_usec)
            annotations_data['end_time_usec'].append(end_time_usec)
        
        # Create DataFrame from the collected data
        annotations_df = pd.DataFrame(annotations_data)
        
        # Create metadata dictionary
        metadata_dict = {
            'sampling_rate': sampling_rate,
            'start_time_usec': start_time_usec,
            'end_time_usec': end_time_usec,
            'duration_sec': duration_sec,
            'edf_path': edf_path,
            'file_name': Path(edf_path).name
        }

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

    # def save_metadata(self, record_id, session, dataset_path, path_to_save: Path = Path(__file__).parent.parent / 'data'):
    #     """Save the metadata to a file.
        
    #     Args:
    #         record_id: The ID of the record
    #         dataset_name: Name of the dataset
    #         path_to_save: Path where metadata will be saved. Defaults to 'data'
    #     """
    #     #print(path_to_save)


    #     output_filepath = Path(path_to_save / record_id / session)
    #     print(f"Saving metadata to {output_filepath}")
    #     output_filepath.mkdir(parents=True, exist_ok=True)

    #     channels_df.to_csv(output_filepath / 'channels.csv', index=False)
    #     annotations_df.to_csv(output_filepath / 'annotations.csv', index=False)
    #     clips_df.to_csv(output_filepath / 'clips.csv', index=False)
    #     with open(output_filepath / 'metadata.txt', 'w') as f:
    #         for key, value in metadata_dict.items():
    #             f.write(f"{key}: {value}\n")

    #     return channels_df, annotations_df, metadata_dict, clips_df
    
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
        "sub-RID0037",
        "sub-RID0102",
        "sub-RID0213",
        "sub-RID0309",
        "sub-RID0420",
        "sub-RID0459",
        "sub-RID0502",
        "sub-RID0529",
        # "sub-RID0534",
        "sub-RID0536",
        "sub-RID0583",
        "sub-RID0646",
        "sub-RID0652",
        # "sub-RID0786",
        # "sub-RID0825",
        # "sub-RID0839",
        "sub-RID0490",
        "sub-RID0572",
        "sub-RID0648",
        "sub-RID0194",
        "sub-RID0476",
        "sub-RID0596" ]

    ieeg = IEEGmetadata()
    ieeg_data_df = ieeg.get_redcap_data(subjects=subjects_to_find)
    ieeg_data_df = ieeg.expand_ieeg_days_rows(ieeg_data_df)

    for record_id, data in ieeg_data_df.iterrows():
        dataset_name = data['ieegportalsubjno']
        ieeg.save_metadata(record_id=record_id, dataset_name=dataset_name)

# %%
