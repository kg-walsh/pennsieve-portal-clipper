#%%
import pandas as pd
import numpy as np
from ieeg_metadata_validated import IEEGmetadataValidated
from pathlib import Path
from IPython import embed

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
        annotations_to_remove = r"(?i)(\*?Tech notation: Video/EEG monitoring taking place|\binterictal\b)"
        annotations = annotations[~annotations['description'].str.contains(annotations_to_remove, case=False)]
        
        # Reset clip fields and check overlaps
        clip['has_events'] = False
        clip['events'] = ''
        clip['annotators'] = ''
        clip['layers'] = ''
        clip['close_to_event'] = False
        clip_clean = self._check_clip_overlaps(clip, annotations)
        
        # Apply conditions again
        conditions = ~clip_clean['close_to_event'] & ~clip_clean['is_night']
        is_day_1 = clip_clean['timestamp'].str.contains(r'Day 1\b')
        return clip_clean[conditions & ~is_day_1]
    
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
                
                # For each continuous segment
                for segment_num, segment in day_clips.groupby(segment_id):
                    
                    # If segment duration is <= 1 hour
                    segment_duration = segment['time'].max() - segment['time'].min()
                    if segment_duration <= pd.Timedelta(hours=1):
                        # Mark these clips for extraction
                        interictal_clips.loc[segment.index, 'mark_for_extraction'] = False
            
            # Remove the final formatting line since 'timestamp' is not a datetime column
            interictal_clips = interictal_clips.drop(columns=['day_label', 'time'])

            # Create a summary string with RID and save to text file
            summary = f"Summary for {self.record_id} or {clip_path.parent.name}:\n"
            summary += interictal_clips.groupby('day_num')['mark_for_extraction'].value_counts().to_string()
            print(summary)

            # Save summary to text file
            output_path = Path(__file__).parent.parent / 'data' / 'interictal_clips' / f'{self.record_id}_{clip_path.parent.name}_summary.txt'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(summary)

            return interictal_clips

# %%
if __name__ == '__main__':

    subjects_to_find = ['sub-RID0031', 'sub-RID0032', 'sub-RID0033', 'sub-RID0050',
       'sub-RID0051', 'sub-RID0064', 'sub-RID0089', 'sub-RID0101',
       'sub-RID0117', 'sub-RID0143', 'sub-RID0167', 'sub-RID0175',
       'sub-RID0179', 'sub-RID0193', 'sub-RID0222', 'sub-RID0238',
       'sub-RID0267', 'sub-RID0301', 'sub-RID0320', 'sub-RID0322',
       'sub-RID0332', 'sub-RID0381', 'sub-RID0405', 'sub-RID0412',
       'sub-RID0424', 'sub-RID0508', 'sub-RID0562', 'sub-RID0589',
       'sub-RID0595', 'sub-RID0621', 'sub-RID0658', 'sub-RID0675',
       'sub-RID0679', 'sub-RID0700', 'sub-RID0785', 'sub-RID0796',
       'sub-RID0852', 'sub-RID0883', 'sub-RID0893', 'sub-RID0941',
       'sub-RID0967']
    
    for subject in subjects_to_find:
        clip_generator = ClipGenerator(record_id=subject)
        interictal_clips = clip_generator.mark_interictal_clips()

# %%