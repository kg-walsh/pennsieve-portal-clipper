#%%
from ieeg.auth import Session
import os
import numpy as np
import pandas as pd
from typing import Tuple, Dict
from ieeg_metadata import IEEGmetadata
from manualvalidation_data import ManualValidation
from clip_generator import ClipGenerator
from pathlib import Path
from IPython import embed
import mne
import json
import re

#%%
class IEEGmetadataValidated(ManualValidation, ClipGenerator):
    """
    A class that inherits from both IEEGmetadata and ManualValidation.
    """

    def __init__(self):
        """
        Initialize both parent classes
        Set up IEEG session
        """
        super().__init__()

        # get data environment
        self.data_path = Path(os.getenv('RAW_DATA_DIR'))
        self.output_path = Path(__file__).parent.parent / 'data' #Path(os.getenv('OUTPUT_DIR'))

    # Main entry point
    def process_subject_data(self, subject_id: str) -> None:
        """
        Process all data for a single subject.
        
        Args:
            subject_id (str): Subject ID in format 'sub-RID0222'
        """
        # Get patient information
        sessions_df = self.get_available_sessions(self.data_path, [subject_id])
        start_times_df =  self.get_actual_start_times(record_id=[subject_id])
        seizure_times_df = self.get_seizure_times(record_id=[subject_id])
        print(seizure_times_df)

        # check for multiple sessions or no seizure times
        if len(sessions_df) > 1 and 'session' not in seizure_times_df.columns:
            print(f"Warning: Multiple sessions found for {subject_id}, but no session information provided for seizure markings.")
            return        
        if seizure_times_df.empty:
            print(f"Warning: No seizure times found for {subject_id}.")
            return

        # Process each session
        for idx, row_tuple in enumerate(sessions_df.iterrows(), start=2):
            _, data = row_tuple
            
            # get record_id from the patient_id column
            record_id = data['patient_id']
            session = data['session']
            
            self.process_session(
                record_id=record_id,
                session_info=session,
                start_times_df=start_times_df,
                seizure_times_df=seizure_times_df,
                #seizure_times_manual=seizure_times_manual,
            )
    

    def process_seizure_annotations(self, seizure_times: pd.DataFrame) -> pd.DataFrame:
        """
        Convert seizure times from manual validation into a standardized annotation format.
        
        Args:
            seizure_times (pd.DataFrame): DataFrame containing seizure timing information
                Expected columns: ['source', 'start', 'end']
        
        Returns:
            pd.DataFrame: Annotations DataFrame with columns:
                - layer: always 'manual_validation'
                - annotator: source of the seizure annotation
                - description: always 'seizure'
                - type: always 'seizure'
                - start_time_usec: seizure start time in microseconds
                - end_time_usec: seizure end time in microseconds
        """
        # Create annotations DataFrame from seizure times
        annotations = {
            'layer': ['manual_validation'] * len(seizure_times),
            'annotator': seizure_times['source'].tolist() if 'source' in seizure_times.columns else 'Unknown',
            'description': ['seizure'] * len(seizure_times),
            'type': ['seizure'] * len(seizure_times),
            # Convert seconds to microseconds (1e6)
            'start_time_usec': (seizure_times['start'] * 1e6).astype(int).tolist(),
            'end_time_usec': (seizure_times['end'] * 1e6).astype(int).tolist()
        }
        
        # Convert to DataFrame and sort by start time
        annotations_manual = pd.DataFrame(annotations)
        annotations_manual = annotations_manual.sort_values(by='start_time_usec')
        
        return annotations_manual
    
    def process_session(self, record_id, session_info, seizure_times_df=None, 
                        start_times_df=None, max_edfs=1):
        """
        Simple function to process all EDF files for a session and create session-level outputs.
        
        Parameters:
        record_id: Patient ID
        session_info: Either a session string (e.g., 'ses-implant01') or a dataframe row with session details
        seizure_times_df: DataFrame containing seizure timing information
        start_times_df: DataFrame containing start times for each EDF
        max_edfs: Maximum number of EDFs to process (None = process all)
        """
        # Determine session identifier
        session = session_info
        
        # Get available EDFs for this session
        edf_df = self.get_available_edf_files(self.data_path, record_id, only_pulled=True, session=session)
        print(f'Found {len(edf_df)} EDFs for {record_id}, session {session}')
        
        if edf_df.empty:
            print(f"No EDFs found for {record_id}, session {session}. Skipping.")
            return
        
        # Limit number of EDFs to process if specified
        if max_edfs is not None and max_edfs > 0:
            edf_df = edf_df.head(max_edfs)
        
        # Get all seizure annotations
        seizure_annotations = self.process_seizure_annotations(seizure_times_df)
        
        # Create session output directory
        session_output_dir = Path(self.output_path) / record_id / session
        session_output_dir.mkdir(parents=True, exist_ok=True)
        
        # init containers for consolidated data
        edf_annotations = []
        all_clips = []
        first_edf_channels = None
        channels_consistent = True
        
        # Session metadata
        session_metadata = {
            'record_id': record_id,
            'session': session,
            'edf_files': []
        }
        
        # Process each EDF file
        for idx, row in edf_df.iterrows():
            edf_path = Path(row['edf_path'])
            edf_file = row['edf_file']
            
            # Extract file number (day/run) from filename
            file_number = self._extract_day_or_run(edf_file)
            if file_number is None:
                print(f"Warning: Unable to extract file number from {edf_file}.")
                return
            
            print(f'Processing {edf_file} (File #{file_number})')

            # filter for seizures in this file
            #edf_seizures_annotations = seizure_annotations[int(seizure_annotations['Day']) == file_number]
            edf_seizures_annotations = seizure_annotations
            print(edf_seizures_annotations)

            # Get metadata and data for this EDF
            channels_df, annotations_df, metadata_dict, raw_edf = self.get_dataset_metadata(edf_path)

            # Get combined annotations
            annotations_df_validated = pd.concat(
            [annotations_df, edf_seizures_annotations], 
            ignore_index=True
            )

            clips_df = self._ieeg_clips(annotations_df_validated, metadata_dict)
            clips_df['file_num'] = file_number
            clips_df = self.timestamp_clips(clips_df, metadata_dict)

            run_dir = self.save_metadata(
                record_id=record_id,
                session=session,
                run_name=f"run{file_number}",
                channels_df=channels_df,
                annotations_df=annotations_df_validated,
                metadata_dict=metadata_dict,
                clips_df=clips_df,
            )

            print('metadata saved')

            clips_df = self.get_dataset_clips(record_id, edf_path, run_dir, clips_df, raw_edf)

            # Add file_number to EDF annotations
            #annotations_df['file_number'] = file_number
            
        #     # Filter seizure annotations for this file if "Day" column exists
        #     file_seizure_annotations = None
        #     if seizure_times_df is not None and not seizure_times_df.empty and 'Day' in seizure_times_df.columns:
        #         # Filter seizures for this specific day/file
        #         file_seizures = seizure_times_df[seizure_times_df['Day'] == file_number]
        #         if not file_seizures.empty:
        #             # Process these file-specific seizures
        #             file_seizure_annotations = self.process_seizure_annotations(file_seizures)
        #             file_seizure_annotations['file_number'] = file_number
        #             print(f"  Added {len(file_seizure_annotations)} seizures for file #{file_number}")
                    
        #             # Combine with EDF annotations
        #             annotations_df = pd.concat([annotations_df, file_seizure_annotations], ignore_index=True)
            
        #     # Process clips based on combined annotations
        #     clips_df = self._ieeg_clips(annotations_df, metadata_dict)
        #     clips_df['file_number'] = file_number
            
        #     # Add timestamp information carefully
        #     try:
        #         if 'actual_start_time' in metadata_dict and metadata_dict['actual_start_time'] is not None:
        #             clips_df = self.timestamp_clips(clips_df, metadata_dict)
        #     except Exception as e:
        #         print(f"Warning: Error in timestamp_clips: {e}")
            
        #     # Add to EDF-specific annotations and clips
        #     edf_annotations.append(annotations_df)
        #     all_clips.append(clips_df)
            
        #     # Add to session metadata
        #     session_metadata['edf_files'].append({
        #         'file_number': file_number,
        #         'edf_file': edf_file,
        #         'start_time_usec': metadata_dict['start_time_usec'],
        #         'end_time_usec': metadata_dict['end_time_usec'],
        #         'duration_sec': metadata_dict['duration_sec'],
        #         'sampling_rate': metadata_dict['sampling_rate']
        #     })
                

        # # Create consolidated files
        # if edf_annotations:
        #     # Add channels consistency to metadata
        #     session_metadata['channels_consistent'] = channels_consistent
            
        #     # Concatenate EDF annotations
        #     consolidated_edf_annotations = pd.concat(edf_annotations, ignore_index=True)
            
        #     # Combine EDF annotations with seizure annotations (if any)
        #     if seizure_annotations is not None and not seizure_annotations.empty:
        #         consolidated_annotations = pd.concat([consolidated_edf_annotations, seizure_annotations], ignore_index=True)
        #     else:
        #         consolidated_annotations = consolidated_edf_annotations
            
        #     # Concatenate all clips
        #     consolidated_clips = pd.concat(all_clips, ignore_index=True)
            
        #     # Save consolidated data
        #     consolidated_annotations.to_csv(session_output_dir / 'all_annotations.csv', index=False)
        #     consolidated_clips.to_csv(session_output_dir / 'all_clips.csv', index=False)
        #     if first_edf_channels is not None:
        #         first_edf_channels.to_csv(session_output_dir / 'all_channels.csv', index=False)
            
        #     # Save session metadata
        #     with open(session_output_dir / 'session_metadata.json', 'w') as f:
        #         json.dump(session_metadata, f, indent=4)
            
        #     print(f"Session processing complete. Consolidated {len(edf_annotations)} EDF files.")
        # else:
        #     print("No data processed for this session.")

    def _extract_day_or_run(self, filename):
        """Extract day or run number from filename"""
        # Pattern for run-XXXX format
        run_pattern = r'run-0*(\d+)'
        run_match = re.search(run_pattern, filename)
        
        if run_match:
            return int(run_match.group(1))
        
        # Pattern for dayX format
        day_pattern = r'day0*(\d+)'
        day_match = re.search(day_pattern, filename)
        
        if day_match:
            return int(day_match.group(1))
        
        return None
    
    def check_edf_start_time(self, file_datetime):

        # Check if it's the default date
        if file_datetime.year == 1985 and file_datetime.month == 1 and file_datetime.day == 1:
            time_part = None
        else:
            time_part = file_datetime.time()

        return time_part                        

    def timestamp_clips(self, clips_df: pd.DataFrame, metadata_dict: Dict) -> pd.DataFrame:
        """
        Index clips DataFrame with timestamps and night/day information at 1-minute intervals.
        
        Args:
            clips_df (pd.DataFrame): DataFrame containing clips data
            metadata_dict (Dict): Dictionary containing metadata

        Returns:
            pd.DataFrame: Clips DataFrame with timestamp index and night/day information
        """
        
        # Convert actual_start_time string to datetime
        if metadata_dict['actual_start_time'] is not None:
            actual_start_time = pd.to_datetime(metadata_dict['actual_start_time'])
            
            # Create timestamp index
            timestamps = []
            is_night = []
            for t in clips_df.start_time_usec:
                t = t/1e6 # Convert to seconds
                days_elapsed = int(t // (24 * 3600))
                current_time = actual_start_time + pd.Timedelta(seconds=int(t))
                
                # Format for index
                day_str = f"Day {days_elapsed + 1} {current_time.strftime('%H:%M:%S')}"
                timestamps.append(day_str)
                
                # Check if current time is during night hours (19:00-08:00)
                hour = current_time.hour
                is_night.append(hour >= 19 or hour < 8)
            
            # Reindex the DataFrame to 1-minute intervals
            clips_df.index = pd.Index(timestamps, name='timestamp')
            clips_df['is_night'] = is_night
        else:
            # No actual start time available, just return the original DataFrame
            print("No actual start time available. Skipping timestamp indexing.")
        
        return clips_df
    
    def save_validated_metadata(self, record_id, dataset_name, 
                      annotations_df_validated=None, 
                      clips_df_validated=None,
                      metadata_dict=None,
                      path_to_save: Path = Path(__file__).parent.parent / 'data'):
        """Save the metadata to a file.
        
        Args:
            record_id: The ID of the record
            dataset_name: Name of the dataset
            path_to_save: Path where metadata will be saved. Defaults to 'data'
        """
        Path(path_to_save / record_id / dataset_name).mkdir(parents=True, exist_ok=True)

        annotations_df_validated.to_csv(Path(path_to_save) / record_id / dataset_name / 'annotations.csv', index=False)
        clips_df_validated.to_csv(Path(path_to_save) / record_id / dataset_name / 'clips.csv')
        with open(Path(path_to_save) / record_id / dataset_name / 'metadata.txt', 'w') as f:
            for key, value in metadata_dict.items():
                f.write(f"{key}: {value}\n")

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
        raw = mne.io.read_raw_edf(edf_path, preload=True)
        
        # Get channel information
        channel_labels = raw.ch_names
        channel_indices = list(range(len(channel_labels)))
        sampling_rate = raw.info['sfreq']
        
        # Get timing information
        file_datetime = raw.info['meas_date']
        file_actual_start_time = self.check_edf_start_time(file_datetime)
        file_start_time_usec = 0  # EDF files typically start at 0
        file_duration_sec = raw.times[-1]  # Duration in seconds
        file_end_time_usec = int(file_duration_sec * 1e6)  # Convert to microseconds
        
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
            'actual_start_time': file_actual_start_time,
            'start_time_usec': file_start_time_usec,
            'end_time_usec': file_end_time_usec,
            'duration_sec': file_duration_sec,
            'edf_path': edf_path,
            'file_name': Path(edf_path).name
        }

        return channels_df, annotations_df, metadata_dict, raw

    def get_dataset_clips(self, record_id, edf_path, output_dir, clips_df, raw_edf):
        clip_generator = ClipGenerator(record_id=subject)
        print('init of clip generator')
        clip_generator.find_interictal_clips(output_dir)
        interictal_clips = clip_generator.mark_interictal_clips(output_dir, raw_edf)
    
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
        print(clips_df)
        
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

    def save_metadata(self, record_id, session, run_name, channels_df, annotations_df, metadata_dict, clips_df, 
                    path_to_save: Path = Path(__file__).parent.parent / 'data'):
        """Save the metadata to a file.
            
        Args:
            record_id: The ID of the record
            session: Session identifier
            run_name: Name of the run
            channels_df: DataFrame containing channel information
            annotations_df: DataFrame containing annotations
            metadata_dict: Dictionary containing metadata
            clips_df: DataFrame containing clip information
            path_to_save: Path where metadata will be saved. Defaults to 'data'
        """
        print(path_to_save)
        output_filepath = Path(path_to_save / record_id / session / run_name)
        print(f"Saving metadata to {output_filepath}")
        output_filepath.mkdir(parents=True, exist_ok=True)
        
        channels_df.to_csv(output_filepath / 'channels.csv', index=False)
        annotations_df.to_csv(output_filepath / 'annotations.csv', index=False)
        clips_df.to_csv(output_filepath / 'clips.csv', index=False)
        metadata_file = output_filepath / 'metadata.txt'
        print('data saved')
        with open(metadata_file, 'w') as f:
            for key, value in metadata_dict.items():
                f.write(f"{key}: {value}\n")
            
        return output_filepath 

# %%
if __name__ == '__main__':
    
    #subjects_to_find = ["sub-RID0572"]
    subjects_to_find = ["sub-PENN019"]
    
    ieeg = IEEGmetadataValidated()
    
    for subject in subjects_to_find:
        print(f'Processing {subject}')
        try:
            ieeg.process_subject_data(subject)
        except Exception as e:
            print(f'Error processing {subject}: {e}')

# %%

