#%%
import pandas as pd
import numpy as np
#from ieeg_metadata_validated import IEEGmetadataValidated
from pathlib import Path
import h5py
from IPython import embed
from loguru import logger
from typing import Tuple, Dict
import os

# %%
class ClipGenerator():
    """
    A class that inherits from IEEGmetadataValidated.
    """

    def __init__(self, record_id: str, 
                 data_path = Path(__file__).parent.parent / 'data'):
        """
        Initialize the ClipGenerator.
        """
        super().__init__()
        self.record_id = record_id
        self.data_path = self.output_path = Path(os.getenv('OUTPUT_DIR'))
        
        # Configure loguru logger
        logger.add(
            "clip_generator.log",
            rotation="100 MB",  # Rotate file when it reaches 100MB
            retention="1 week",  # Keep logs for 1 week
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            level="INFO"
        )

    def _find_interictal_clips(self, output_dir):
        """
        Find the interictal clips.
        """
        print('finding interictal clips')
        dir_path = output_dir #self.data_path / self.record_id
        all_interictal_clips = []

        for clip_path in dir_path.rglob('*clips.csv'):
            print(clip_path)
            clip = pd.read_csv(clip_path)
            
            # Apply initial interictal conditions
            if 'is_night' in clip.columns:
                is_day_1 = clip['timestamp'].str.contains(r'Day 1\b')
                conditions = ~clip['close_to_event'] & ~clip['is_night'] & ~is_day_1
            else:
                conditions = ~clip['close_to_event']
            clips_interictal = clip[conditions]
            
            # If no clips found, try processing with annotations
            #if clips_interictal.empty:
            #    clips_interictal = self._remove_redundant_annotations(clip, clip_path)
            
            if not clips_interictal.empty:
                all_interictal_clips.append(clips_interictal)
                #output_path = output_dir / 'clips_interictal.csv'
                #clips_interictal.to_csv(output_path, index=False)
            else:
                print(f'No interictal clips found for {self.record_id}')
        
        # Combine all interictal clips
        if all_interictal_clips:
            return pd.concat(all_interictal_clips, ignore_index=True)
        else:
            return None

    def _remove_redundant_annotations(self, clip: pd.DataFrame, clip_path: Path) -> pd.DataFrame:
        """
        Remove redundant annotations from the clips.
        
        Args:
            clip (pd.DataFrame): Original clips dataframe
            clip_path (Path): Path to the clips file
        
        Returns:
            pd.DataFrame: Filtered interictal clips
        """
        annotations_path = clip_path.parent / 'annotations.csv'
        annotations = pd.read_csv(annotations_path)
        annotations_to_remove = r"(?i)(\*?Tech notation: Video/EEG monitoring taking place|\binterictal\b|x)"
        annotations = annotations[~annotations['description'].str.contains(annotations_to_remove, case=False, na=False)]
        
        # Reset clip fields and check overlaps
        clip['has_events'] = False
        clip['events'] = ''
        clip['annotators'] = ''
        clip['layers'] = ''
        clip['close_to_event'] = False
        clip_clean = self._check_clip_overlaps(clip, annotations, hours_window=2)

        # Apply conditions again
        conditions = ~clip_clean['close_to_event'] & ~clip_clean['is_night']
        is_day_1 = clip_clean['timestamp'].str.contains(r'Day 1\b')

        clip_clean = clip_clean[conditions & ~is_day_1]
        
        return clip_clean
    
    def mark_interictal_clips(self, output_dir, raw_edf, clip_duration_minutes=5):
        """
        Find interictal clips and mark one continuous segment per hour for extraction.
        
        Args:
            output_dir: Directory containing the clips data
            raw_edf: Raw EDF data object
            clip_duration_minutes (int): Duration of each segment in minutes. Default is 5.
        
        Returns:
            list: List of segment dictionaries for extraction
        """
        # Step 1: Find interictal clips
        interictal_clips = self._find_interictal_clips(output_dir)
        
        if interictal_clips is None or interictal_clips.empty:
            print(f'No interictal clips found for {self.record_id}')
            logger.warning(f'No interictal clips found for {self.record_id}')
            return None

        # Step 2: Initialize segments list
        segments = []
        
        # Step 3: Process each file
        if 'file_num' in interictal_clips.columns:
            for file_num, file_clips in interictal_clips.groupby('file_num'):
                # Sort by start_time_usec
                file_clips = file_clips.sort_values('start_time_usec')
                
                # Get file start time
                file_start_time = file_clips['start_time_usec'].min()
                
                # Create hour window identifier for each clip
                hour_window = ((file_clips['start_time_usec'] - file_start_time) // (3600 * 1e6)).astype(int)
                
                # Process each hour window separately
                for hour, hour_clips in file_clips.groupby(hour_window):
                    # Skip if fewer clips than needed for the specified duration
                    if len(hour_clips) < clip_duration_minutes:
                        print(f"Hour {hour} in file {file_num}: only {len(hour_clips)} clips, need {clip_duration_minutes}")
                        continue
                    
                    # Find continuous segments within this hour
                    hour_clips = hour_clips.sort_values('start_time_usec')
                    one_minute_usec = 60 * 1e6  # 1 minute in microseconds
                    time_diff = hour_clips['start_time_usec'].diff()
                    new_segment = time_diff > one_minute_usec
                    segment_id = new_segment.cumsum()
                    
                    # Find the length of segments
                    segment_lengths = hour_clips.groupby(segment_id).size()
                    
                    # Find segments with at least the required number of clips
                    valid_segments = segment_lengths[segment_lengths >= clip_duration_minutes]
                    
                    if valid_segments.empty:
                        print(f"Hour {hour} in file {file_num}: no continuous segment of {clip_duration_minutes} minutes found")
                        continue  # No segment long enough in this hour
                    
                    # Find the longest segment in this hour
                    longest_segment = valid_segments.idxmax()
                    
                    # Get the clips in the longest segment
                    longest_hour_segment = hour_clips[segment_id == longest_segment]
                    
                    # Take the middle N clips from this segment
                    segment_length = len(longest_hour_segment)
                    middle_start = (segment_length - clip_duration_minutes) // 2
                    clips_to_extract = longest_hour_segment.iloc[middle_start:middle_start + clip_duration_minutes]

                    # Create a segment entry
                    segment_start = clips_to_extract.iloc[0]['start_time_usec']
                    segment_end = clips_to_extract.iloc[clip_duration_minutes-1]['end_time_usec']

                    # Add segment to the list
                    segments.append({
                        'start_time_usec': segment_start,
                        'end_time_usec': segment_end,
                        'file_num': file_num,
                        'duration_minutes': clip_duration_minutes
                    })
                    
                    print(f"Selected {clip_duration_minutes}-minute segment from hour {hour}, file {file_num}")
        else:
            print("Warning: 'file_num' column not found in clips data")
        
        # Step 4: Extract the segments
        self._get_interictal_clips(segments, output_dir, raw_edf)
        
        return segments
    
    def mark_ictal_clips(self, output_dir, raw_edf, clip_preictal=True, preictal_type="fixed", preictal_mins=5):
        """
        Find and mark ictal clips for extraction, optionally including preictal periods.
        
        Parameters:
            output_dir: Directory to save output
            raw_edf: Path to raw EDF data
            clip_preictal: If True, also include preictal periods
            preictal_type: "fixed" for fixed duration or "matching" to match ictal duration
            preictal_mins: Minutes of preictal data if using fixed duration
        
        Returns:
            pd.DataFrame: Ictal and optional preictal clips with marked segments
        """
        
        # Find ictal clips directly
        ictal_clips = self._find_ictal_clips(output_dir)
        
        if ictal_clips is None or ictal_clips.empty:
            print(f'No ictal clips found for {self.record_id}')
            return None
        
        # Initialize the mark_for_extraction column - mark all as True
        ictal_clips['mark_for_extraction'] = True
        
        # If requested, add preictal periods
        if clip_preictal:
            preictal_clips = self._find_preictal_clips(ictal_clips, preictal_type, preictal_mins)
            
            # Mark all preictal clips for extraction
            if preictal_clips is not None and not preictal_clips.empty:
                preictal_clips['mark_for_extraction'] = True
                preictal_clips['is_preictal'] = True
                
                # Mark the original clips as ictal (not preictal)
                ictal_clips['is_preictal'] = False
                
                # Combine ictal and preictal clips
                combined_clips = pd.concat([ictal_clips, preictal_clips], ignore_index=True)
                combined_clips = combined_clips.sort_values(['file_num', 'start_time_usec'])
            else:
                combined_clips = ictal_clips
                combined_clips['is_preictal'] = False
        else:
            combined_clips = ictal_clips
            combined_clips['is_preictal'] = False
        
        # Extract the marked clips
        self._get_ictal_clips(combined_clips, output_dir, raw_edf)
        
        return combined_clips

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

    def _find_ictal_clips(self, output_dir):
        """
        Find the ictal clips from annotations without saving to file.
        
        Returns:
            pd.DataFrame: DataFrame of ictal clips
        """
        print('Finding ictal clips')
        dir_path = output_dir
        
        all_ictal_clips = []
        
        for annotation_path in dir_path.rglob('*annotations.csv'):
            print(f"Processing annotations: {annotation_path}")
            annotations = pd.read_csv(annotation_path)
            
            # Filter for manual_validation layer
            manual_validations = annotations[annotations['layer'] == 'manual_validation']
            
            if manual_validations.empty:
                print(f"No manual_validation annotations found in {annotation_path}")
                continue
            
            # Process each seizure annotation
            for _, annotation in manual_validations.iterrows():
                # Extract start and end times
                start_time_usec = annotation['start_time_usec']
                end_time_usec = annotation['end_time_usec']
                duration_usec = end_time_usec - start_time_usec
                
                # Create a clip for the entire seizure
                ictal_clip = {
                    'start_time_usec': start_time_usec,
                    'end_time_usec': end_time_usec,
                    'duration_usec': duration_usec,
                    'type': annotation['type'],
                    'description' : annotation['description'],
                    'annotator': annotation['annotator'],
                    'is_seizure': True,
                    'file_num' : annotation['file_num']

                }
                
                all_ictal_clips.append(ictal_clip)
        
        # Create DataFrame from all clips
        if all_ictal_clips:
            return pd.DataFrame(all_ictal_clips)
            print('added all ictal clips')
        else:
            return None
        
    def _find_preictal_clips(self, ictal_clips, preictal_type="fixed", preictal_mins=10):
        """
        Generate preictal clips from the ictal clips.
        
        Parameters:
            ictal_clips: DataFrame of ictal clips
            preictal_type: "fixed" for fixed duration or "matching" to match ictal duration
            preictal_mins: Minutes of preictal data if using fixed duration
            
        Returns:
            pd.DataFrame: DataFrame of preictal clips
        """
        if ictal_clips is None or ictal_clips.empty:
            return None
        
        preictal_clips = []
        
        for _, seizure in ictal_clips.iterrows():
            if preictal_type == "fixed":
                # Fixed duration preictal period
                preictal_usec = preictal_mins * 60 * 1e6  # Convert minutes to microseconds
            else:  # "matching"
                # Match the duration of the seizure
                preictal_usec = seizure['duration_usec']
            
            # Calculate preictal start time
            preictal_start = seizure['start_time_usec'] - preictal_usec
            file_num = seizure['file_num']
            
            # Create preictal clip
            preictal_clip = {
                'start_time_usec': preictal_start,
                'end_time_usec': seizure['start_time_usec'],
                'duration_usec': preictal_usec,
                'description': f"Preictal for {seizure['description']}",
                'type': 'preictal',
                'is_seizure': False,
                'file_num' : file_num

            }
            
            # Copy other relevant fields
            for field in ['annotator', 'file_num']:
                if field in seizure and not pd.isna(seizure[field]):
                    preictal_clip[field] = seizure[field]
            
            preictal_clips.append(preictal_clip)
        
        if preictal_clips:
            return pd.DataFrame(preictal_clips)
        else:
            return None

    def _get_interictal_clips(self, segments: list, output_dir: Path, raw_edf):
        """
        Get the interictal segments and save them to separate H5 files for each day.
        """
        if not segments:
            print(f"No segments to extract for {self.record_id}")
            return
        
        # Group segments by file_num
        segments_by_file = {}
        for segment in segments:
            file_num = segment['file_num']
            if file_num not in segments_by_file:
                segments_by_file[file_num] = []
            segments_by_file[file_num].append(segment)

        # Process each file separately
        for file_num, file_segments in segments_by_file.items():
            logger.info(f'Processing {len(file_segments)} segments for file {file_num} of {self.record_id}')
            
            # Create a separate H5 file for each day
            with h5py.File(output_dir / f'interictal_ieeg_run{file_num}.h5', 'w') as f:
                # Process each 5-minute segment
                for segment_idx, segment in enumerate(file_segments, start=1):
                    start_time_usec = segment['start_time_usec']
                    end_time_usec = segment['end_time_usec']
                    
                    # Extract the entire 5-minute segment as one continuous clip
                    ieeg_segment, sampling_rate, channel_labels = self.get_single_clip(
                        raw_edf=raw_edf, 
                        start_time_usec=start_time_usec, 
                        end_time_usec=end_time_usec
                    )
                    
                    segment_num = f'{segment_idx:02d}'
                    # Create dataset for the entire 5-minute segment
                    ieeg_dataset = f.create_dataset(f'segment{segment_num}', data=ieeg_segment)
                    # Add attributes to the dataset
                    ieeg_dataset.attrs['start_time_usec'] = start_time_usec
                    ieeg_dataset.attrs['end_time_usec'] = end_time_usec
                    ieeg_dataset.attrs['duration_minutes'] = 5
                    ieeg_dataset.attrs['channels_labels'] = channel_labels
                    ieeg_dataset.attrs['sampling_rate'] = sampling_rate

    def _get_ictal_clips(self, ictal_clips: pd.DataFrame, output_dir: Path, raw_edf):
        """
        Get the ictal/preictal clips and save them to separate H5 files for each day.
        """
        # Filter to only marked clips
        ictal_clips = ictal_clips[ictal_clips['mark_for_extraction']]

        print('Getting ictal clips')
        
        if ictal_clips.empty:
            print(f"No ictal clips marked for extraction for {self.record_id}")
            logger.warning(f"No ictal clips marked for extraction for {self.record_id}")
            return
        
        # Process each file separately
        for file_num, file_clips in ictal_clips.groupby('file_num'):
            print(f'Processing file {file_num} for {self.record_id}')
            logger.info(f'Processing file {file_num} for {self.record_id}')
            
            # Create separate H5 files for ictal and preictal clips
            ictal_only = file_clips[~file_clips['is_preictal']]
            preictal_only = file_clips[file_clips['is_preictal']]

            print(f'ICTAL ONLY CLIPS: {ictal_only}')
            
            # Process ictal clips
            if not ictal_only.empty:
                with h5py.File(output_dir / f'ictal_ieeg_file{file_num}.h5', 'w') as f:
                    for clip_idx, (index, clip) in enumerate(ictal_only.iterrows(), start=1):
                        start_time_usec = clip['start_time_usec']
                        end_time_usec = clip['end_time_usec']
                        
                        ieeg_clip, sampling_rate, channel_labels = self.get_single_clip(
                            raw_edf=raw_edf, 
                            start_time_usec=start_time_usec, 
                            end_time_usec=end_time_usec
                        )
                        
                        clip_num = f'{clip_idx:02d}'
                        # Create dataset directly in the root of the file
                        ieeg_dataset = f.create_dataset(f'seizure{clip_num}', data=ieeg_clip)
                        # Add attributes to the dataset
                        ieeg_dataset.attrs['start_time_usec'] = start_time_usec
                        ieeg_dataset.attrs['end_time_usec'] = end_time_usec
                        ieeg_dataset.attrs['channels_labels'] = channel_labels
                        ieeg_dataset.attrs['sampling_rate'] = sampling_rate
                        
                        # Add seizure-specific attributes
                        if 'description' in clip:
                            ieeg_dataset.attrs['description'] = clip['description']
                        if 'type' in clip:
                            ieeg_dataset.attrs['seizure_type'] = clip['type']
                        if 'annotator' in clip:
                            ieeg_dataset.attrs['annotator'] = clip['annotator']
            
            # Process preictal clips
            if not preictal_only.empty:
                with h5py.File(output_dir / f'preictal_ieeg_file{file_num}.h5', 'w') as f:
                    for clip_idx, (index, clip) in enumerate(preictal_only.iterrows(), start=1):
                        start_time_usec = clip['start_time_usec']
                        end_time_usec = clip['end_time_usec']
                        
                        ieeg_clip, sampling_rate, channel_labels = self.get_single_clip(
                            raw_edf=raw_edf, 
                            start_time_usec=start_time_usec, 
                            end_time_usec=end_time_usec
                        )
                        
                        clip_num = f'{clip_idx:02d}'
                        # Create dataset directly in the root of the file
                        ieeg_dataset = f.create_dataset(f'preictal{clip_num}', data=ieeg_clip)
                        # Add attributes to the dataset
                        ieeg_dataset.attrs['start_time_usec'] = start_time_usec
                        ieeg_dataset.attrs['end_time_usec'] = end_time_usec
                        ieeg_dataset.attrs['channels_labels'] = channel_labels
                        ieeg_dataset.attrs['sampling_rate'] = sampling_rate
                        
                        # Add related seizure attributes
                        if 'description' in clip:
                            ieeg_dataset.attrs['related_seizure_description'] = clip['description']

    def get_single_clip(self, raw_edf, start_time_usec: int, end_time_usec: int) -> Tuple[pd.DataFrame, float, list[str]]:
        """Get dataset metadata from IEEG."""
            # Convert microseconds to seconds for MNE
        start_time_sec = start_time_usec / 1e6
        end_time_sec = end_time_usec / 1e6
        
        # Get sampling rate and channel labels
        sampling_rate = raw_edf.info['sfreq']
        channel_labels = raw_edf.ch_names
        
        # Calculate sample indices
        start_sample = int(start_time_sec * sampling_rate)
        end_sample = int(end_time_sec * sampling_rate)
        
        # Extract data
        ieeg_clip = raw_edf.get_data(start=start_sample, stop=end_sample)

        return ieeg_clip, sampling_rate, channel_labels

# %% 
if __name__ == '__main__':
    
    subjects_to_find = ['sub-RID0839',
            'sub-RID0786',
            'sub-RID0646',
            'sub-RID0825','sub-RID0596']
    
    for subject in subjects_to_find:
        try:
            clip_generator = ClipGenerator(record_id=subject)
            logger.info(f"Processing subject: {subject}")
            clip_generator.find_interictal_clips()
            interictal_clips = clip_generator.mark_interictal_clips()
        except Exception as e:
            logger.error(f"Error processing {subject}: {str(e)}")

# %%
