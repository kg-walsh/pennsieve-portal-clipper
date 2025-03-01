# IEEG Data Clipper

A comprehensive Python package for extracting, processing, and validating iEEG (intracranial electroencephalography) data from IEEG.org portal. This tool streamlines the workflow for researchers working with epilepsy data by providing automated data retrieval, manual validation support, and intelligent clip generation.

## Features

- 🔄 Automated IEEG.org data retrieval and processing
- ✅ Manual validation integration with Google Sheets
- 📊 REDCap integration for metadata management
- 🎬 Smart clip generation with annotation mapping
- 🌓 Automatic day/night classification
- 📋 Comprehensive interictal period identification
- 📈 Multi-format data export (CSV, JSON)

## Prerequisites

- Python 3.6+
- Active IEEG.org account
- REDCap API access token
- Google Cloud credentials (for Sheets API)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/n-sinha/ieeg-portal-clipper.git
cd ieeg-portal-clipper
```

2. Install dependencies:
```bash
pip install -e .
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
# Create and edit .env file
cp .env.example .env
```

Required environment variables:
```bash
IEEG_USERNAME=your_username
IEEG_PASSWORD=your_password
REDCAP_TOKEN=your_token
REDCAP_REPORT_ID=your_report_id
SHEET_ID_MANUAL_VALIDATION=your_sheet_id
SHEET_NAME_MANUAL_VALIDATION_START_TIME=your_sheet_name
SHEET_NAME_MANUAL_VALIDATION_SEIZURE_TIME=your_sheet_name
```

## Module Documentation

### IEEGMetadata (`src/ieeg_metadata.py`)
Handles core IEEG.org portal data retrieval and processing.

```python
class IEEGMetadata(Redcap):
    """
    Core class for retrieving and processing IEEG.org metadata.
    
    Methods:
    - setup_ieeg_session(): Initializes IEEG portal session
    - get_dataset_metadata(dataset_name): Retrieves channel and annotation information
    - _ieeg_clips(annotations_df, metadata_dict): Creates 1-minute clips with annotations
    - _check_clip_overlaps(clips_df, annotations_df): Checks for event overlaps in clips
    - save_metadata(record_id, dataset_name): Saves metadata to files
    """
```

### IEEGMetadataValidated (`src/ieeg_metadata_validated.py`)
Extends IEEGMetadata with manual validation support.

```python
class IEEGMetadataValidated(IEEGMetadata, ManualValidation):
    """
    Handles validated IEEG data processing with manual annotations.
    
    Methods:
    - process_subject_data(subject_id): Processes all data for a single subject
    - process_seizure_annotations(seizure_times): Converts manual seizure times to annotations
    - timestamp_clips(clips_df, metadata_dict): Adds timestamps and day/night information
    - save_validated_metadata(): Saves validated metadata to files
    """
```

### ManualValidation (`src/manualvalidation_data.py`)
Manages manual validation data from Google Sheets.

```python
class ManualValidation(Redcap):
    """
    Handles manual validation annotations from Google Sheets.
    
    Methods:
    - _get_record_id(hup_id): Converts HUP ID to record ID
    - get_actual_start_times(record_id): Retrieves recording start times
    - get_seizure_times(record_id): Retrieves manually validated seizure times
    """
```

### Redcap (`src/redcap_data.py`)
Handles REDCap data retrieval and processing.

```python
class Redcap:
    """
    Manages REDCap data access and processing.
    
    Methods:
    - get_redcap_data(report_id, subjects): Fetches data from REDCap
    - expand_ieeg_days_rows(df): Expands rows with D-number ranges
    """
```

### REDCapInterface (`src/redcap.py`)
Manages REDCap integration for additional metadata.

```python
class REDCapInterface:
    """
    Handles REDCap API interactions.
    
    Methods:
    - get_patient_data(record_id): Retrieves patient records
    - update_record(record_id, data): Updates existing records
    - validate_data(data): Validates data format
    """
```

### ClipGenerator (`src/clip_generator.py`)
Handles clip generation and processing.

```python
class ClipGenerator:
    """
    Creates and processes data clips.
    
    Methods:
    - generate_clips(dataset, annotations): Creates clips from annotations
    - classify_day_night(clip): Classifies clips by time
    - validate_clip_quality(clip): Checks clip quality
    """
```

## Output Structure

```
data/
└── [record_id]/
    └── [dataset_name]/
        ├── channels.csv
        ├── annotations.csv
        ├── clips.csv
        └── metadata.txt
```

Each file contains:
- `channels.csv`: Channel labels and indices
- `annotations.csv`: All annotations including seizure times and manual validations
- `clips.csv`: Information about generated data clips
- `metadata.txt`: Key-value pairs of dataset metadata including sampling rate, start/end times, and duration

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Citation

If you use this package in your research, please cite:
```bibtex
@software{ieeg_data_clipper,
    title = {IEEG Data Clipper},
    author = {Sinha, N.},
    year = {2023},
    url = {https://github.com/n-sinha/ieeg-portal-clipper}
}
```
