#%%
import pandas as pd
import numpy as np
#from ieeg_metadata_validated import IEEGmetadataValidated
from pathlib import Path
import h5py
from IPython import embed
from loguru import logger
from typing import Tuple, Dict

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
        self.data_path = data_path
        
        # Configure loguru logger
        logger.add(
            "clip_generator.log",
            rotation="100 MB",  # Rotate file when it reaches 100MB
            retention="1 week",  # Keep logs for 1 week
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            level="INFO"
        )

    def find_interictal_clips(self, output_dir):
        """
        Find the interictal clips.
        """
        print('finding interictal clips')
        dir_path = output_dir #self.data_path / self.record_id
        print(dir_path)
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
            if clips_interictal.empty:
                clips_interictal = self._remove_redundant_annotations(clip, clip_path)
            
            if not clips_interictal.empty:
                output_path = output_dir / 'clips_interictal.csv'
                clips_interictal.to_csv(output_path, index=False)
            else:
                print(f'No interictal clips found for {self.record_id}')

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
    
    def mark_interictal_clips(self, output_dir, raw_edf):
        """
        Get the interictal clips and mark continuous 1-hour segments for extraction.
        
        Returns:
            pd.DataFrame: Interictal clips with marked segments for extraction
        """
        dir_path =  output_dir #self.data_path / self.record_id
        
        for clip_path in dir_path.rglob('*clips_interictal.csv'):
            # Read the clips
            interictal_clips = pd.read_csv(clip_path)
            
            # Initialize the mark_for_extraction column
            interictal_clips['mark_for_extraction'] = True
           
            # # Split the timestamp into day number and time
            # interictal_clips[['day_label', 'day_num', 'time']] = interictal_clips['timestamp'].str.split(expand=True)
            
            # # Convert time to datetime, keeping track of day number separately
            # interictal_clips['time'] = pd.to_datetime(interictal_clips['time'], format='%H:%M:%S')
            # interictal_clips['day_num'] = interictal_clips['day_num'].astype(int)
            
            # # Group by day_num instead of day
            # for day_num, day_clips in interictal_clips.groupby('day_num'):
            if 'file_num' in interictal_clips.columns:
                # Sort by start_time_usec 
                interictal_clips = interictal_clips.sort_values('start_time_usec')
                
                # Find continuous segments using start_time_usec
                time_diff = interictal_clips['start_time_usec'].diff()
                one_minute_usec = 60 * 1e6  # 1 minute in microseconds
                new_segment = time_diff > one_minute_usec
                segment_id = new_segment.cumsum()
                
                # Find the length of segments
                segment_lengths = interictal_clips.groupby(segment_id).size()
                
                # Find the longest segment
                longest_segment = segment_lengths.idxmax()
                
                # Mark all segments that are not the longest segment as False
                not_longest_segment_mask = (segment_id != longest_segment)
                interictal_clips.loc[interictal_clips[not_longest_segment_mask].index, 'mark_for_extraction'] = False
                
                # For the longest segment, mark everything after 30 minutes as False
                longest_segment_mask = (segment_id == longest_segment)
                segment_indices = interictal_clips[longest_segment_mask].index
                if len(segment_indices) > 30:
                    interictal_clips.loc[segment_indices[30:], 'mark_for_extraction'] = False
            else:
                print(f"Warning: 'file_num' column not found in {clip_path}")
                
                # interictal_clips = interictal_clips.drop(columns=['day_label', 'time'])
                
            self._get_interictal_clips(interictal_clips, clip_path.parent, raw_edf)
                            
        return interictal_clips
        
    def _get_interictal_clips(self, interictal_clips: pd.DataFrame, clip_path: Path, raw_edf):
        """
        Get the interictal clips and save them to separate H5 files for each day.
        """
        dataset = clip_path.name
        interictal_clips = interictal_clips[interictal_clips['mark_for_extraction']]
        
        # Process each day separately
        for file_num, file_clips in interictal_clips.groupby('file_num'):
            logger.info(f'Processing day {file_num} in {dataset} of {self.record_id}')
            # Create a separate H5 file for each day
            with h5py.File(clip_path / f'interictal_ieeg_day{file_num}.h5', 'w') as f:
                # Use enumerate to get a counter for the clips
                for clip_idx, (index, clip) in enumerate(file_clips.iterrows(), start=1):
                    start_time_usec = clip['start_time_usec']
                    end_time_usec = clip['end_time_usec']
                    
                    ieeg_clip, sampling_rate, channel_labels = self.get_single_clip(
                        raw_edf=raw_edf, 
                        start_time_usec=start_time_usec, 
                        end_time_usec=end_time_usec
                    )
                    
                    clip_num = f'{clip_idx:02d}'
                    # Create dataset directly in the root of the file
                    ieeg_dataset = f.create_dataset(f'clip{clip_num}', data=ieeg_clip)
                    # Add attributes to the dataset
                    #ieeg_dataset.attrs['timestamp'] = clip['timestamp']
                    ieeg_dataset.attrs['start_time_usec'] = start_time_usec
                    ieeg_dataset.attrs['end_time_usec'] = end_time_usec
                    ieeg_dataset.attrs['channels_labels'] = channel_labels
                    ieeg_dataset.attrs['sampling_rate'] = sampling_rate

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
