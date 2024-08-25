import pandas as pd
from sqlalchemy import create_engine

# Database connection strings
SOURCE_DATABASE = 'postgresql://admin:admin@localhost:5432/source'
DWH_DATABASE = 'postgresql://admin:admin@localhost:5432/dwh'

def extract_data(table):
    """
    Extract data from a specified table in the source database.
    
    Args:
        table (str): Name of the table to extract data from.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    engine = create_engine(SOURCE_DATABASE)
    query = f"SELECT * FROM {table}"
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"An error occurred while extracting data from {table}: {e}")
        return None

def load_data(df, target_table):
    """
    Load data into the target table in the data warehouse.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be loaded.
        target_table (str): Name of the target table in the data warehouse.
    """
    engine = create_engine(DWH_DATABASE)
    try:
        df.to_sql(target_table, engine, if_exists='replace', index=False, method='multi')
        print(f"Data successfully loaded into {target_table}.")
    except Exception as e:
        print(f"An error occurred while loading data into {target_table}: {e}")

def create_marketing_mart():
    """
    Create the marketing data mart by merging reservations with campaign data.
    """
    # Extract data from source tables
    reservations = extract_data('Reservations')
    campaigns = extract_data('Campaign')

    if reservations is not None and campaigns is not None:
        # Ensure that the columns used for merging have the same data type
        reservations['voucher_code'] = reservations['voucher_code'].astype(str)
        campaigns['id'] = campaigns['id'].astype(str)

        # Merge reservations with campaign data
        marketing_mart = reservations.merge(campaigns, left_on='voucher_code', right_on='id', how='left')

        # Prepare final DataFrame with relevant columns and rename them
        marketing_mart = marketing_mart[['id_x', 'reservation_datetime', 'check_in_date', 'check_out_date', 
                                         'total_room_price', 'total_discount', 'name', 'description']]
        marketing_mart.columns = ['reservation_id', 'reservation_datetime', 'check_in_date', 'check_out_date', 
                                  'total_room_price', 'total_discount', 'campaign_name', 'campaign_description']

        # Load the data into the marketing mart table
        load_data(marketing_mart, 'marketing_mart')

def create_finance_mart():
    """
    Create the finance data mart by merging reservations with payment data.
    """
    # Extract data from source tables
    reservations = extract_data('Reservations')
    payments = extract_data('Payments')
    
    if reservations is not None and payments is not None:
        # Merge reservations with payment data
        finance_mart = reservations.merge(payments, on='id', how='left')
        
        # Prepare final DataFrame with relevant columns and rename them
        finance_mart = finance_mart[['id', 'reservation_datetime', 'total_room_price', 'amount', 'payment_datetime']]
        finance_mart.columns = ['reservation_id', 'reservation_datetime', 'total_room_price', 'payment_amount', 'payment_datetime']
        
        # Load the data into the finance mart table
        load_data(finance_mart, 'finance_mart')

def create_hotel_branch_mart():
    """
    Create the hotel branch data mart by merging stays with room data.
    """
    # Extract data from source tables
    stays = extract_data('Stays')
    rooms = extract_data('Rooms')
    
    if stays is not None and rooms is not None:
        # Merge stays with room data
        hotel_branch_mart = stays.merge(rooms, left_on='room_id', right_on='id', how='left')
        
        # Prepare final DataFrame with relevant columns and rename them
        hotel_branch_mart = hotel_branch_mart[['id_x', 'date', 'reference_reservation_id', 'room_id', 'name', 'room_type', 'floor', 'hotel_id']]
        hotel_branch_mart.columns = ['stay_id', 'stay_date', 'reservation_id', 'room_id', 'room_name', 'room_type', 'floor', 'hotel_id']
        
        # Load the data into the hotel branch mart table
        load_data(hotel_branch_mart, 'hotel_branch_mart')

def main():
    """
    Main function to create all data marts.
    """
    create_marketing_mart()
    create_finance_mart()
    create_hotel_branch_mart()

if __name__ == "__main__":
    main()
