import pandas as pd
from sqlalchemy import create_engine

SOURCE_DATABASE = 'postgresql://admin:admin@localhost:5432/source'
LOAD_DATABASE = 'postgresql://admin:admin@localhost:5432/dwh'

def extract_data(source_system):
    engine = create_engine(SOURCE_DATABASE)
    query = f"SELECT * FROM {source_system}"
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"An error occurred while extracting data from {source_system}: {e}")
        return None

def transform_dimension_data(df, dimension_type):
    if dimension_type == 'dim_rooms':
        df['room_type'] = df['room_type'].str.lower().str.replace('[^a-z]', '', regex=True)
    elif dimension_type == 'dim_guests':
        df['phone_number'] = df['phone_number'].apply(lambda x: f"+62{x}" if x and not x.startswith('+') else x)
    return df

def prepare_data(df, dimension_type):
    columns_map = {
        'dim_reservations': {'id': 'reservation_id'},
        'dim_stays': {'id': 'stay_id', 'reference_reservation_id': 'reservation_id'},
        'dim_rooms': {'id': 'room_id'},
        'dim_guests': {'id': 'guest_id'}
    }
    
    df = df.rename(columns=columns_map.get(dimension_type, {}))
    
    # Ensure that necessary columns are present
    if 'effective_date' not in df.columns:
        df['effective_date'] = pd.Timestamp.now().normalize()
    if 'end_date' not in df.columns:
        df['end_date'] = None
    if 'current_flag' not in df.columns:
        df['current_flag'] = True
        
    return df

def load_dimension_data(df, target_table):
    engine = create_engine(LOAD_DATABASE)
    
    try:
        # Load data into the database, replacing the existing table
        df.to_sql(target_table, engine, if_exists='replace', index=False, method='multi')
        print(f"Data successfully loaded into {target_table}.")
    except Exception as e:
        print(f"An error occurred while loading data into {target_table}: {e}")

def main():
    # Extract data for dimension tables
    reservations_df = extract_data('Reservations')
    stays_df = extract_data('Stays')
    rooms_df = extract_data('Rooms')
    guests_df = extract_data('Users')
    
    if reservations_df is None or stays_df is None or rooms_df is None or guests_df is None:
        print("Extraction failed, aborting ETL process.")
        return

    # Transform data for dimension tables
    rooms_df = transform_dimension_data(rooms_df, 'dim_rooms')
    guests_df = transform_dimension_data(guests_df, 'dim_guests')

    # Prepare data for dimension tables
    reservations_df = prepare_data(reservations_df, 'dim_reservations')
    stays_df = prepare_data(stays_df, 'dim_stays')
    rooms_df = prepare_data(rooms_df, 'dim_rooms')
    guests_df = prepare_data(guests_df, 'dim_guests')

    # Load dimension data into the data warehouse
    load_dimension_data(reservations_df, 'dim_reservations')
    load_dimension_data(stays_df, 'dim_stays')
    load_dimension_data(rooms_df, 'dim_rooms')
    load_dimension_data(guests_df, 'dim_guests')

if __name__ == "__main__":
    main()
