import pandas as pd
from sqlalchemy import create_engine

# Database connection strings
SOURCE_DATABASE = 'postgresql://admin:admin@localhost:5432/source'
LOAD_DATABASE = 'postgresql://admin:admin@localhost:5432/dwh'

def extract_data(source_system):
    """
    Extract data from a specified source table in the source database.
    
    Args:
        source_system (str): Name of the source table to extract data from.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    engine = create_engine(SOURCE_DATABASE)
    query = f"SELECT * FROM {source_system}"
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"An error occurred while extracting data from {source_system}: {e}")
        return None

def transform_room_type(df):
    """
    Transform the 'room_type' column by normalizing it to lowercase and removing non-alphabetic characters.
    
    Args:
        df (pd.DataFrame): DataFrame containing room data.

    Returns:
        pd.DataFrame: DataFrame with transformed 'room_type' column.
    """
    if 'room_type' in df.columns:
        df['room_type'] = df['room_type'].str.lower().str.replace('[^a-z]', '', regex=True)
    return df

def transform_phone_number(df):
    """
    Transform the 'phone_number' column to the standardized Indonesian format (+62).
    
    Args:
        df (pd.DataFrame): DataFrame containing user data.

    Returns:
        pd.DataFrame: DataFrame with transformed 'phone_number' column.
    """
    if 'phone_number' in df.columns:
        df['phone_number'] = df['phone_number'].apply(lambda x: f"+62{x}" if x and not x.startswith('+') else x)
    return df

def transform_reservation_data(df):
    """
    Transform reservation data, ensuring numeric columns are of type float.
    
    Args:
        df (pd.DataFrame): DataFrame containing reservation data.

    Returns:
        pd.DataFrame: DataFrame with transformed numeric columns.
    """
    if 'total_room_price' in df.columns:
        df['total_room_price'] = df['total_room_price'].astype(float)
    if 'total_discount' in df.columns:
        df['total_discount'] = df['total_discount'].astype(float)
    return df

def prepare_fact_data(df, columns, id_col, new_id_col):
    """
    Prepare fact data by selecting relevant columns, removing duplicates, and renaming the ID column.
    
    Args:
        df (pd.DataFrame): DataFrame to be prepared.
        columns (list): List of columns to select.
        id_col (str): Name of the existing ID column.
        new_id_col (str): Name of the new ID column.

    Returns:
        pd.DataFrame: Prepared DataFrame ready for loading.
    """
    df = df[columns].drop_duplicates(subset=id_col)
    df = df.rename(columns={id_col: new_id_col})
    return df

def load_data(df, target_table):
    """
    Load data into the specified target table in the data warehouse.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data to be loaded.
        target_table (str): Name of the target table in the data warehouse.
    """
    engine = create_engine(LOAD_DATABASE)
    try:
        df.to_sql(target_table, engine, if_exists='append', index=False, method='multi')
        print(f"Data successfully loaded into {target_table}.")
    except Exception as e:
        print(f"An error occurred while loading data into {target_table}: {e}")

def main():
    """
    Main function to execute the ETL process: extract, transform, merge, and load data.
    """
    # Extract data from source tables
    reservations_df = extract_data('Reservations')
    stays_df = extract_data('Stays')
    rooms_df = extract_data('Rooms')
    users_df = extract_data('Users')
    
    # Check if extraction was successful
    if reservations_df is None or stays_df is None or rooms_df is None or users_df is None:
        print("Extraction failed, aborting ETL process.")
        return
    
    # Transform data
    rooms_df = transform_room_type(rooms_df)
    users_df = transform_phone_number(users_df)
    reservations_df = transform_reservation_data(reservations_df)
    
    # Merge dataframes
    stays_df = pd.merge(stays_df, rooms_df[['id', 'room_type']], left_on='room_id', right_on='id', how='left', suffixes=('', '_room'))
    stays_df = pd.merge(stays_df, users_df[['id', 'phone_number']], left_on='guest_id', right_on='id', how='left', suffixes=('', '_guest'))
    stays_df = stays_df.rename(columns={'reference_reservation_id': 'reservation_id'})
    
    reservations_df = pd.merge(reservations_df, stays_df[['reservation_id']], left_on='id', right_on='reservation_id', how='left', suffixes=('', '_stays'))
    
    # Handle missing reservations for stays
    valid_stays = stays_df[stays_df['reservation_id'].isin(reservations_df['id'])]
    invalid_stays = stays_df[~stays_df['reservation_id'].isin(reservations_df['id'])]
    
    # Log invalid stays for further investigation
    if not invalid_stays.empty:
        print("Invalid stays found (no matching reservation):")
        print(invalid_stays)
    
    # Prepare fact tables
    fact_reservations = prepare_fact_data(reservations_df, 
        ['id', 'reservation_datetime', 'check_in_date', 'check_out_date', 'status', 'hotel_id', 'booker_id', 'total_room_price', 'voucher_code', 'total_discount'], 
        'id', 'reservation_id')
    
    fact_stays = prepare_fact_data(valid_stays, 
        ['id', 'date', 'reservation_id', 'room_id', 'guest_id'], 
        'id', 'stay_id')
    
    # Load data into the data warehouse
    load_data(fact_reservations, 'fact_reservations')
    load_data(fact_stays, 'fact_stays')

if __name__ == "__main__":
    main()
