# load imports for data gathering
import json
import pandas as pd # loads pandas library
import requests # loads requests library
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb
import sqlite3 # sqlalchemy for PostgreSQL
import os

# If running in a Jupyter notebook, use '%matplotlib inline' instead.
plt.ion()  # Enable interactive mode for matplotlib

# function to read the available files from the folder
def process_raw_directory(directory_path):
    """
    Scans the directory and handles each file based on its type.
    """
    directory_path ="C:/Users/Davie/Documents/GitHub/ETL_Data_Warehouse_Project/data/raw/"
    
    loaded_data = {}
    sql_scripts = []

    # 1. Iterate through every file in your raw folder
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        # Get the file extension 
        ext = os.path.splitext(filename)[1].lower()
        
        try:
            # Handle CSV Data (e.g., twitter_archive_enhanced.csv)
            if ext == '.csv':
                print(f"Loading CSV Data: {filename}")
                loaded_data[filename] = pd.read_csv(file_path)
            
            # Handle TSV Data 
            elif ext == '.tsv':
                print(f"Loading TSV Data: {filename}")
                loaded_data[filename] = pd.read_csv(file_path, sep='\t')
            
            # Handle JSON/TXT Data 
            elif ext in ['.txt', '.json']:
                print(f"Loading JSON Data: {filename}")
                loaded_data[filename] = pd.read_json(file_path, lines=True)
            
            # Handle SQL Files 
            elif ext == '.sql':
                print(f"Detected SQL Script: {filename}")

                with open(file_path, 'r') as f:
                    sql_scripts.append({'filename': filename, 'content': f.read()})
            
            else:
                print(f"Skipping unsupported file type: {filename}")

        except Exception as e:
            print(f"Error processing {filename}: {e}")

    return loaded_data, sql_scripts

# --- Execution ---
raw_folder = 'raw'
data_frames, warehouse_scripts = process_raw_directory(raw_folder)

# Function to load datasets
def load_single_file(file_name):
    """
    Professional loader that reads a file from the raw directory 
    by automatically detecting its format from the file extension.
    """
    base_path = "C:/Users/Davie/Documents/GitHub/ETL_Data_Warehouse_Project/data/raw/"
    full_path = os.path.join(base_path, file_name)
    
    # Extract the extension (e.g., '.csv', '.tsv')
    _, extension = os.path.splitext(file_name)
    format_lower = extension.lower()
   
    try:
        if format_lower == '.csv':
            # twitter_archive_enhanced.csv
            return pd.read_csv(full_path)
        
        elif format_lower == '.tsv':
            # image_predictions.tsv
            return pd.read_csv(full_path, sep='\t')
        
        elif format_lower in ['.json', '.txt']:
            # tweet_json.txt
            return pd.read_json(full_path, lines=True)
            
        elif format_lower == '.xlsx':
            return pd.read_excel(full_path, engine='openpyxl')
        
        else:
            print(f"Format '{format_lower}' in file '{file_name}' is not supported yet.")
            return None
            
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        return None


# 1. Load texts

df_tweets = load_single_file('tweet_json.txt')
print(df_tweets)

# Save txt to postgresql
df_tweets = load_single_file('tweet_json.txt')

if df_tweets is not None:
    cols = ['created_at', 'id', 'id_str', 'full_text', 'retweet_count', 'favorited', 'retweeted']
    
    # Filter columns, drop duplicates, and drop rows with missing IDs
    tweets = df_tweets[cols].drop_duplicates()
    tweets = tweets.dropna(subset=['id'])
    
    print(f"Standardized {len(tweets)} unique tweets.")

    processed_dir = "C:/Users/Davie/Documents/GitHub/ETL_Data_Warehouse_Project/data/processed/"
    
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    output_file = os.path.join(processed_dir, "processed_tweets.sql")

    # Generate a SQL script with PostgreSQL syntax
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("-- PostgreSQL Processed Data Table --\n")
        f.write("CREATE TABLE IF NOT EXISTS tweets (\n")
        f.write("    created_at TIMESTAMP,\n")
        f.write("    id BIGINT PRIMARY KEY,\n")
        f.write("    id_str VARCHAR(255),\n")
        f.write("    full_text TEXT,\n")
        f.write("    retweet_count INT,\n")
        f.write("    favorited BOOLEAN,\n")
        f.write("    retweeted BOOLEAN\n")
        f.write(");\n\n")

        # Create Insert Statements for each row
        for _, row in tweets.iterrows():
            # handle single quotes in text to prevent SQL errors
            clean_text = str(row['full_text']).replace("'", "''")
            
            sql = f"INSERT INTO tweets VALUES ('{row['created_at']}', {row['id']}, '{row['id_str']}', " \
                  f"'{clean_text}', {row['retweet_count']}, {row['favorited']}, {row['retweeted']}) " \
                  f"ON CONFLICT (id) DO NOTHING;\n"
            f.write(sql)

    print(f"Professional PostgreSQL script saved to: {output_file}")


# 2. Load the csv

df_csv = load_single_file('twitter_archive_enhanced.csv')
print(df_csv)

# Saving csv to postgresql
df_csv = load_single_file('twitter_archive_enhanced.csv')

if df_csv is not None:
    colsv = ['tweet_id', 'timestamp', 'source', 'text', 'retweeted_status_id', 
             'retweeted_status_user_id', 'retweeted_status_timestamp', 
             'expanded_urls', 'rating_numerator', 'rating_denominator', 
             'name', 'doggo', 'floofer', 'pupper', 'puppo']
    
    # Select columns
    tweet_archive = df_csv[colsv]
    
    tweet_archive = tweet_archive.drop_duplicates(subset=['tweet_id'])
    
    tweet_archive = tweet_archive.dropna(subset=['tweet_id', 'text'])

    print(f"Standardized Archive: {len(tweet_archive)} records prepared.")

    processed_dir = "C:/Users/Davie/Documents/GitHub/ETL_Data_Warehouse_Project/data/processed/"
    output_file = os.path.join(processed_dir, "processed_archive.sql")

    # Generate PostgreSQL-compatible script
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("-- PostgreSQL Archive Table --\n")
        f.write("CREATE TABLE IF NOT EXISTS twitter_archive (\n")
        f.write("    tweet_id BIGINT PRIMARY KEY,\n")
        f.write("    timestamp TIMESTAMP,\n")
        f.write("    source TEXT,\n")
        f.write("    tweet_text TEXT,\n")
        f.write("    rating_numerator INT,\n")
        f.write("    rating_denominator INT,\n")
        f.write("    dog_stage VARCHAR(50)\n") # Standardizing doggo/pupper/etc
        f.write(");\n\n")

        for _, row in tweet_archive.head(100).iterrows(): # Doing 100 for the demo script
            clean_text = str(row['text']).replace("'", "''")
            sql = f"INSERT INTO twitter_archive (tweet_id, timestamp, tweet_text) " \
                  f"VALUES ({row['tweet_id']}, '{row['timestamp']}', '{clean_text}') " \
                  f"ON CONFLICT (tweet_id) DO NOTHING;\n"
            f.write(sql)

    print(f"Archive SQL script saved to: {output_file}")


# 3. Load xlsx

df_xlsx = load_single_file('league_table.xlsx')
print(df_xlsx)

# Save xlsx in postgresql format
df_xlsx = load_single_file('league_table.xlsx')

if df_xlsx is not None:
    colsx = ['Club', 'MP', 'W', 'D', 'L', 'Pts', 'GF', 'GA', 'GD', 'avg_goals', 'Pos']
    
    # Select columns and perform cleaning
    league_table = df_xlsx[colsx].copy()

    # Drop duplicates
    league_table = league_table.drop_duplicates()
    league_table = league_table.dropna(subset=['Club', 'Pos'])
    
    # Ensuring Pos and Pts are integers for the warehouse
    league_table['Pos'] = league_table['Pos'].astype(int)
    league_table['Pts'] = league_table['Pts'].astype(int)

    print(f"Standardized League Table: {len(league_table)} clubs processed.")

    processed_dir = "C:/Users/Davie/Documents/GitHub/ETL_Data_Warehouse_Project/data/processed/"
    output_file = os.path.join(processed_dir, "processed_league_table.sql")

    # PostgreSQL-compatible script
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("-- PostgreSQL League Table --\n")
        f.write("CREATE TABLE IF NOT EXISTS league_table (\n")
        f.write("    pos INT PRIMARY KEY,\n")
        f.write("    club VARCHAR(255),\n")
        f.write("    mp INT,\n")
        f.write("    w INT,\n")
        f.write("    d INT,\n")
        f.write("    l INT,\n")
        f.write("    pts INT,\n")
        f.write("    gf INT,\n")
        f.write("    ga INT,\n")
        f.write("    gd INT,\n")
        f.write("    avg_goals NUMERIC(10, 2)\n")
        f.write(");\n\n")

        # Create Insert Statements
        for _, row in league_table.iterrows():
            # Handle apostrophes in club names (if applicable)
            clean_club = str(row['Club']).replace("'", "''")
            
            sql = (f"INSERT INTO league_table VALUES ({row['Pos']}, '{clean_club}', "
                   f"{row['MP']}, {row['W']}, {row['D']}, {row['L']}, {row['Pts']}, "
                   f"{row['GF']}, {row['GA']}, {row['GD']}, {row['avg_goals']}) "
                   f"ON CONFLICT (pos) DO UPDATE SET pts = EXCLUDED.pts;\n")
            f.write(sql)

    print(f"League SQL script saved to: {output_file}")


# 4. Load TSV

df_tsv = load_single_file('image_predictions.tsv')
print(df_tsv)

# Save TSV into postgresql
df_tsv = load_single_file('image_predictions.tsv')

if df_tsv is not None:
    colst = ['tweet_id', 'jpg_url', 'img_num', 'p1', 'p1_dog', 'p2', 'p2_dog', 'p3', 'p3_dog']
    
    tsv = df_tsv[colst].copy()
    
    # Remove duplicate predictions based on tweet_id
    tsv = tsv.drop_duplicates(subset=['tweet_id'])
    
    # Drop rows missing the primary key (tweet_id) or the image link
    tsv = tsv.dropna(subset=['tweet_id', 'jpg_url'])
    
    # Standardize text
    for col in ['p1', 'p2', 'p3']:
        tsv[col] = tsv[col].str.replace('_', ' ').str.title()

    print(f"Standardized Image Predictions: {len(tsv)} records processed.")

    processed_dir = "C:/Users/Davie/Documents/GitHub/ETL_Data_Warehouse_Project/data/processed/"
    output_file = os.path.join(processed_dir, "processed_image_predictions.sql")

    # PostgreSQL-compatible script
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("-- PostgreSQL Image Predictions Table --\n")
        f.write("CREATE TABLE IF NOT EXISTS image_predictions (\n")
        f.write("    tweet_id BIGINT PRIMARY KEY,\n")
        f.write("    jpg_url TEXT,\n")
        f.write("    img_num INT,\n")
        f.write("    p1_breed TEXT,\n")
        f.write("    p1_is_dog BOOLEAN,\n")
        f.write("    p2_breed TEXT,\n")
        f.write("    p2_is_dog BOOLEAN,\n")
        f.write("    p3_breed TEXT,\n")
        f.write("    p3_is_dog BOOLEAN\n")
        f.write(");\n\n")

        # Create Insert Statements
        for _, row in tsv.iterrows():
            # SQL string escaping for breed names
            p1 = str(row['p1']).replace("'", "''")
            p2 = str(row['p2']).replace("'", "''")
            p3 = str(row['p3']).replace("'", "''")
            
            sql = (f"INSERT INTO image_predictions VALUES ({row['tweet_id']}, '{row['jpg_url']}', "
                   f"{row['img_num']}, '{p1}', {row['p1_dog']}, '{p2}', "
                   f"{row['p2_dog']}, '{p3}', {row['p3_dog']}) "
                   f"ON CONFLICT (tweet_id) DO NOTHING;\n")
            f.write(sql)

    print(f"Image Predictions SQL script saved to: {output_file}")



# Using SQLite to run scripts
def build_warehouse(db_name, script_list):
    # Target directory for the database
    db_dir = r'C:\Users\Davie\Documents\GitHub\ETL_Data_Warehouse_Project\sql'
    
    os.makedirs(db_dir, exist_ok=True)

    # Full database path
    db_path = os.path.join(db_dir, db_name)

    # Connect to the database or creates it if it doesn't exist
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"Connecting to Warehouse: {db_path}")

    processed_dir = r"C:\Users\Davie\Documents\GitHub\ETL_Data_Warehouse_Project\data\processed"

    for script_file in script_list:
        path = os.path.join(processed_dir, script_file)
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                cursor.executescript(sql_script)
                print(f"Successfully executed: {script_file}")
        else:
            print(f"File not found: {script_file}")

    conn.commit()
    conn.close()
    print("Data Warehouse Population Complete.")

# Run the automation
scripts_to_run = [
    "processed_tweets.sql",
    "processed_archive.sql",
    "processed_league_table.sql",
    "processed_image_predictions.sql"
]

build_warehouse("My_Data_Warehouse.db", scripts_to_run)