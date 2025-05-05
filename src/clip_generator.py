#%%
import pandas as pd
import numpy as np
from ieeg_metadata_validated import IEEGmetadataValidated
from pathlib import Path
import h5py
from IPython import embed
from loguru import logger

# %%
class ClipGenerator(IEEGmetadataValidated):
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

    def find_interictal_clips(self):
        """
        Find the interictal clips.
        """
        dir_path = self.data_path / self.record_id
        for clip_path in dir_path.rglob('*clips.csv'):
            clip = pd.read_csv(clip_path)
            
            # Apply initial interictal conditions
            conditions = ~clip['close_to_event'] & ~clip['is_night']
            is_day_1 = clip['timestamp'].str.contains(r'Day 1\b')
            clips_interictal = clip[conditions & ~is_day_1]
            
            # If no clips found, try processing with annotations
            if clips_interictal.empty:
                clips_interictal = self._remove_redundant_annotations(clip, clip_path)
            
            if not clips_interictal.empty:
                output_path = clip_path.parent / 'clips_interictal.csv'
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
    
    def mark_interictal_clips(self):
        """
        Get the interictal clips and mark continuous 1-hour segments for extraction.
        
        Returns:
            pd.DataFrame: Interictal clips with marked segments for extraction
        """
        dir_path = self.data_path / self.record_id
        for clip_path in dir_path.rglob('*clips_interictal.csv'):
            # Read the clips
            interictal_clips = pd.read_csv(clip_path)
            
            # Initialize the mark_for_extraction column
            interictal_clips['mark_for_extraction'] = True
           
            # Split the timestamp into day number and time
            interictal_clips[['day_label', 'day_num', 'time']] = interictal_clips['timestamp'].str.split(expand=True)
            
            # Convert time to datetime, keeping track of day number separately
            interictal_clips['time'] = pd.to_datetime(interictal_clips['time'])
            interictal_clips['day_num'] = interictal_clips['day_num'].astype(int)
            
            # Group by day_num instead of day
            for day_num, day_clips in interictal_clips.groupby('day_num'):

                # Sort by time
                day_clips = day_clips.sort_values('time')
                
                # Find continuous segments using time
                time_diff = day_clips['time'].diff()
                new_segment = time_diff > pd.Timedelta(minutes=1)
                segment_id = new_segment.cumsum()
                # find the length of segments
                segment_lengths = day_clips.groupby(segment_id).size()

                # find the longest segment and mark everything except the first 60 minutes as False
                longest_segment = segment_lengths.idxmax()
                
                # Mark all segments that are not the longest segment as False
                not_longest_segment_mask = (segment_id != longest_segment)
                interictal_clips.loc[day_clips[not_longest_segment_mask].index, 'mark_for_extraction'] = False
                
                # For the longest segment, mark everything after 30 minutes as False
                longest_segment_mask = (segment_id == longest_segment)
                segment_indices = day_clips[longest_segment_mask].index
                if len(segment_indices) > 30:
                    interictal_clips.loc[segment_indices[30:], 'mark_for_extraction'] = False
            
            # Remove the final formatting line since 'timestamp' is not a datetime column
            interictal_clips = interictal_clips.drop(columns=['day_label', 'time'])

            self._get_interictal_clips(interictal_clips, clip_path.parent)    

        return interictal_clips
        
    def _get_interictal_clips(self, interictal_clips: pd.DataFrame, clip_path: Path):
        """
        Get the interictal clips and save them to separate H5 files for each day.
        """
        dataset = clip_path.name
        interictal_clips = interictal_clips[interictal_clips['mark_for_extraction']]
        
        # Process each day separately
        for day_num, day_clips in interictal_clips.groupby('day_num'):
            logger.info(f'Processing day {day_num} in {dataset} of {self.record_id}')
            # Create a separate H5 file for each day
            with h5py.File(clip_path / f'interictal_ieeg_day{day_num}.h5', 'w') as f:
                # Use enumerate to get a counter for the clips
                for clip_idx, (index, clip) in enumerate(day_clips.iterrows(), start=1):
                    start_time_usec = clip['start_time_usec']
                    end_time_usec = clip['end_time_usec']
                    
                    ieeg_clip, sampling_rate, channel_labels = self.get_dataset_clips(
                        dataset_name=dataset, 
                        start_time_usec=start_time_usec, 
                        end_time_usec=end_time_usec
                    )
                    
                    clip_num = f'{clip_idx:02d}'
                    # Create dataset directly in the root of the file
                    ieeg_dataset = f.create_dataset(f'clip{clip_num}', data=ieeg_clip)
                    # Add attributes to the dataset
                    ieeg_dataset.attrs['timestamp'] = clip['timestamp']
                    ieeg_dataset.attrs['start_time_usec'] = start_time_usec
                    ieeg_dataset.attrs['end_time_usec'] = end_time_usec
                    ieeg_dataset.attrs['channels_labels'] = channel_labels
                    ieeg_dataset.attrs['sampling_rate'] = sampling_rate

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
