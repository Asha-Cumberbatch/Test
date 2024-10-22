# Data Cleaning and Processing Script for Car Owner Dataset
![datacleaning](datacleaning.jpg)

## Project Overview

This project contains a Python script designed to clean and process a large dataset of car owners with Chinese column names. The data is processed in chunks to improve memory efficiency, and various transformations and validations are applied to ensure data quality.

## Features

1. **Column Renaming**: Maps Chinese column headers to their English equivalents for readability and usability.
2. **Email Normalization**: Converts email addresses to lowercase and strips any surrounding whitespace.
3. **Invalid Email Handling**: Replaces invalid email addresses (those containing patterns like 'noemail' or 'nomail') with 'NULL'.
4. **Duplicate Removal**: Removes duplicate entries based on `ID_Number`, `VIN`, or `Email`, keeping the first occurrence.
5. **Address Merging**: Combines `Address`, `City`, `Province`, and `Postal_Code` into a single `Full_Address` column.
6. **Garbage Collection**: Identifies rows with invalid emails or containing four consecutive commas and moves them to a separate "garbage" file.
7. **Chunked Processing**: Splits the dataset into manageable chunks, processes each chunk individually, and merges the cleaned and garbage data at the end.
8. **Final Output**: Saves both cleaned and garbage data to separate CSV files, along with individual chunk files.

## Project Structure

```
project_directory/
│
├── CarOwnersNationwide.csv          # Original dataset file
├── cleaned_chunk_1.csv              # Cleaned chunk file 1
├── cleaned_chunk_2.csv              # Cleaned chunk file 2
├── cleaned_chunk_3.csv              # Cleaned chunk file 3
├── cleaned_chunk_4.csv              # Cleaned chunk file 4
├── garbage_chunk_1.csv              # Garbage chunk file 1
├── garbage_chunk_2.csv              # Garbage chunk file 2
├── garbage_chunk_3.csv              # Garbage chunk file 3
├── garbage_chunk_4.csv              # Garbage chunk file 4
├── merged_cleaned_data.csv          # Final cleaned data after merging chunks
├── merged_garbage_data.csv          # Final garbage data after merging chunks
├── data_cleaning_script.py          # Python script for data cleaning
└── README.md                        # This README file
```

## Setup

### Prerequisites

- **Python 3.x** installed on your system.
- The following Python libraries should be installed via `pip`:

```bash
pip install pandas numpy
```

### File Path Configuration

Before running the script, ensure the file path to your dataset (`CarOwnersNationwide.csv`) is correctly specified in the script:

```python
file_path = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/CarOwnersNationwide.csv'
```

You can change this path according to your system configuration.

### Running the Script

To run the script:

1. Open a terminal or command prompt.
2. Navigate to the project directory where the script is located.
3. Run the script:

```bash
python data_cleaning_script.py
```

This will process the dataset, split it into chunks, clean each chunk, and output both cleaned and garbage data into CSV files.

### Output

The script will generate the following output files:
- `cleaned_chunk_X.csv`: Cleaned data chunks for each part.
- `garbage_chunk_X.csv`: Garbage data chunks for each part (invalid rows).
- `merged_cleaned_data.csv`: Final merged cleaned data.
- `merged_garbage_data.csv`: Final merged garbage data.

The script also calculates and prints the total number of cleaned and garbage rows, as well as the ratio between them.

## How It Works

1. **Column Renaming**: The Chinese column names in the dataset are mapped to English names for clarity and usability.
2. **Email Normalization and Validation**: Email addresses are normalized by converting them to lowercase and stripping whitespace. Invalid emails, such as those containing patterns like "noemail", are replaced with 'NULL'.
3. **Address Merging**: The columns `Address`, `City`, `Province`, and `Postal_Code` are merged into a new column called `Full_Address` to consolidate location information.
4. **Garbage Data Identification**: Invalid email addresses and rows with four consecutive commas are marked as garbage and saved in separate CSV files.
5. **Chunk Processing**: The dataset is split into four chunks for easier processing. Each chunk is cleaned, and garbage data is separated.
6. **Final Output**: After processing all chunks, the cleaned and garbage data is merged back into full datasets and saved to CSV files.

## Customization

You can modify the script according to your needs. For example:
- Adjust the chunk size by modifying the line `chunk_size = len(data) // 4`.
- Add or remove columns to be dropped by modifying the `columns_to_drop` list.
- Modify the email validation logic or other data cleaning operations as necessary.

## Credits
- Asha Cumberbatch

