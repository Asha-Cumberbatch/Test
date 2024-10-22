import pandas as pd
import numpy as np
import warnings
import os

# Suppress FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Load the dataset with low_memory set to False to prevent DtypeWarning
file_path = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/CarOwnersNationwide.csv'
data = pd.read_csv(file_path, low_memory=False)

# Mapping of Chinese column names to English
column_mapping = {
    '车架号': 'VIN',
    '姓名': 'Name',
    '身份证': 'ID_Number',
    '性别': 'Gender',
    '手机': 'Phone',
    '邮箱': 'Email',
    '省': 'Province',
    '城市': 'City',
    '地址': 'Address',
    '邮编': 'Postal_Code',
    '生日': 'Birthday',
    '行业': 'Industry',
    '月薪': 'Monthly_Salary',
    '婚姻': 'Marital_Status',
    '教育': 'Education',
    'BRAND': 'Brand',
    '车系': 'Car_Series',
    '车型': 'Car_Model',
    '配置': 'Configuration',
    '颜色': 'Color',
    '发动机号': 'Engine_Number'
}

# Rename the columns
data.rename(columns=column_mapping, inplace=True)

# Normalize the email addresses by stripping spaces and converting to lowercase
data['Email'] = data['Email'].str.strip().str.lower()

# Replace the entire email with 'NULL' if it contains 'noemail' or 'nomail'
data['Email'] = data['Email'].replace(to_replace=r'.*(noemai|nomai|noemia|nomea|noemal|noeami|nomei|noma|noemil|noeai).*', value='NULL', regex=True)
  

# Remove duplicates based on 'ID_Number' or 'Email', keeping the first occurrence
data.drop_duplicates(subset=['ID_Number', 'VIN', 'Email'], keep='first', inplace=True)

# Merge Address, City, and Province into one column called "Full_Address"
data['Full_Address'] = data['Address'].astype(str).fillna('') + ', ' + \
                       data['City'].astype(str).fillna('') + ', ' + \
                       data['Province'].astype(str).fillna('') + ',' + \
                       data['Postal_Code'].astype(str).fillna('')

# Save the columns to be dropped into a separate DataFrame
dropped_columns = data[['Postal_Code', 'Province', 'City', 'Address', 'Monthly_Salary', 'Marital_Status', 'Education', 'Color', 'Unnamed: 21', 'Gender', 'Industry', 'Configuration']].copy()

# Drop the specified columns after merging into Full_Address
columns_to_drop = ['Postal_Code', 'Province', 'City', 'Address', 'Monthly_Salary', 'Marital_Status', 'Education', 'Color', 'Unnamed: 21', 'Gender', 'Industry', 'Configuration']
data.drop(columns=columns_to_drop, inplace=True)


# Function to clean data
def clean_data(chunk, dropped_chunk):
    garbage = pd.DataFrame()  # DataFrame to store garbage data
    
    # Reset index for both chunk and dropped_chunk to avoid index issues during concatenation
    chunk.reset_index(drop=True, inplace=True)
    dropped_chunk.reset_index(drop=True, inplace=True)
    
    # Identify rows with invalid emails (excluding NULL)
    invalid_email_mask = ~((chunk['Email'] == 'NULL') | chunk['Email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False))

    # Identify rows with four consecutive commas
    consecutive_comma_mask = chunk.apply(lambda x: ',,,,' in ','.join(x.dropna().astype(str)), axis=1)

    # Combine the invalid email and consecutive comma masks
    combined_mask = invalid_email_mask | consecutive_comma_mask

    # Create a garbage DataFrame for invalid emails or rows with four consecutive commas
    garbage = pd.concat([garbage, chunk[combined_mask]])

    # Align the dropped_chunk index with the main chunk index and add dropped columns to the garbage
    garbage_dropped_columns = dropped_chunk[combined_mask]
    garbage = pd.concat([garbage, garbage_dropped_columns], axis=1)

    # Remove records from the main chunk that are in garbage
    chunk_cleaned = chunk[~combined_mask]

    # Convert 'Phone' column to string and remove non-digit characters
    #chunk_cleaned['Phone'] = chunk_cleaned['Phone'].astype(str).str.replace(r'\D', '', regex=True)

    # Remove duplicates based on 'ID_Number'
    #chunk_cleaned = chunk_cleaned.drop_duplicates(subset=['ID_Number'], keep='first').reset_index(drop=True)
    #garbage = garbage.drop_duplicates(subset=['ID_Number'], keep='first').reset_index(drop=True)

    return chunk_cleaned, garbage



# Split the dataset into chunks, including the dropped columns
chunk_size = len(data) // 4  # Determine chunk size
chunks = np.array_split(data, 4)
dropped_chunks = np.array_split(dropped_columns, 4)

# Specify the full path for saving cleaned and garbage chunks
output_dir = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/'

# Initialize lists to store cleaned and garbage DataFrames
cleaned_chunks_list = []
garbage_chunks_list = []

# Process each chunk
for i, (chunk, dropped_chunk) in enumerate(zip(chunks, dropped_chunks)):
    cleaned_chunk, garbage_chunk = clean_data(chunk, dropped_chunk)
    
    # Append the cleaned and garbage chunks to the lists
    cleaned_chunks_list.append(cleaned_chunk)
    garbage_chunks_list.append(garbage_chunk)

    # Save individual chunk files (optional)
    cleaned_chunk.to_csv(f'{output_dir}cleaned_chunk_{i + 1}.csv', index=False, encoding='utf-8-sig')
    garbage_chunk.to_csv(f'{output_dir}garbage_chunk_{i + 1}.csv', index=False, encoding='utf-8-sig')

    print(f"Cleaned and garbage files for chunk {i + 1} saved successfully.")

# Concatenate all cleaned and garbage chunks
final_cleaned_data = pd.concat(cleaned_chunks_list).reset_index(drop=True)
final_garbage_data = pd.concat(garbage_chunks_list).reset_index(drop=True)

# Save the merged cleaned and garbage files
final_cleaned_data.to_csv(f'{output_dir}merged_cleaned_data.csv', index=False, encoding='utf-8-sig')
final_garbage_data.to_csv(f'{output_dir}merged_garbage_data.csv', index=False, encoding='utf-8-sig')

# Calculate and display the total cleaned and garbage rows
total_cleaned_rows = len(final_cleaned_data)
total_garbage_rows = len(final_garbage_data)

# Calculate and display the ratio of clean to garbage rows
if total_garbage_rows > 0:
    ratio = total_cleaned_rows / total_garbage_rows
else:
    ratio = float('inf')  # Avoid division by zero

print(f"Total cleaned rows: {total_cleaned_rows}")
print(f"Total garbage rows: {total_garbage_rows}")
print(f"Ratio of clean rows to garbage rows: {ratio:.2f}")

print("Merged cleaned and garbage files saved successfully.")
