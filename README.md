# IEEG Data Clipper

A Python package for pulling and processing data from IEEG.org portal, with support for manual validation and clip generation.

## Prerequisites

- Python 3.6+
- IEEG.org account credentials
- REDCap API access (for validation data)
- Google Sheets access (for manual validation)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/n-sinha/ieeg-portal-clipper.git
cd ieeg-portal-clipper
```

2. Install the package and dependencies:
```bash
pip install -e .
pip install requests numpy pandas deprecation pennprov dotenv
```

3. Set up environment variables in `.env` file:
```bash
IEEG_USERNAME=your_username
IEEG_PASSWORD=your_password
REDCAP_TOKEN=your_redcap_token
REDCAP_REPORT_ID=your_report_id
SHEET_ID_MANUAL_VALIDATION=your_sheet_id
SHEET_NAME_MANUAL_VALIDATION_START_TIME=your_sheet_name
SHEET_NAME_MANUAL_VALIDATION_SEIZURE_TIME=your_sheet_name
```

## Usage

1. Basic metadata retrieval:
```python
from ieeg_clips import IEEGmetadata

# Initialize metadata processor
ieeg = IEEGmetadata()

# Get metadata for specific dataset
channels_df, annotations_df, metadata_dict = ieeg.get_dataset_metadata("HUP123_phaseII")
```

2. Process dataset with clips:
```python
from ieeg_clips import IEEGDatasetProcessor

# Initialize processor
processor = IEEGDatasetProcessor()

# Process single dataset
dataset_name = "HUP123_phaseII"
seizure_times, start_times = processor.get_manualvalidation_annotations(dataset_name)
channels_df, annotations_df, metadata_dict = processor.get_dataset_metadata(dataset_name)

# Generate clips
clips_df, interictal_clips = processor.find_dataset_clips(
    dataset_name, annotations_df, metadata_dict
)
```

## Output Structure

The package generates the following directory structure for each dataset:
```
data/
└── HUP123_phaseII/
    ├── all_clips.csv
    ├── interictal_clips.csv
    ├── all_annotations.csv
    └── metadata.txt
```

## Features

- Automated IEEG.org data retrieval
- Manual validation integration
- Clip generation with annotation mapping
- Day/night classification
- Interictal period identification
- REDCap integration for metadata
- Google Sheets integration for validation data

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
