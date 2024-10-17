import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import sys
from config import DATABASE_URI, TABLE_NAME


logging.basicConfig(filename='etl_process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def extract_data(csv_file):
    """
    Extracts data from the specified CSV file.

    :param csv_file: Path to the CSV file.
    :return: DataFrame with extracted data.
    :raises ValueError: If the file is empty or there is an error reading it.
    """
    if os.path.getsize(csv_file) == 0:
        raise ValueError("The file is empty. Unable to extract data.")
    try:
        data = pd.read_csv(csv_file)
        logging.info("Data extracted successfully.")
        return data
    except pd.errors.EmptyDataError:
        raise ValueError("The file is empty. Unable to extract data.")
    except Exception as e:
        raise ValueError(f"An error occurred while reading the file: {e}")


def transform_data(data):
    """
    Transforms the data by removing unnecessary columns and NaN values.

    :param data: DataFrame with extracted data.
    :return: Transformed DataFrame.
    """
    data = data.drop(columns=['Ticket', 'Cabin'], errors='ignore')
    data = data.dropna(subset=['Age', 'Embarked'])
    data.columns = [col.strip().lower() for col in data.columns]
    logging.info("Data transformed successfully.")
    logging.info(f"Number of records after transformation: {len(data)}")
    return data


def load_data(data):
    """
    Loads data into the database.

    :param data: DataFrame with transformed data.
    :raises Exception: If an error occurs while loading data.
    """
    try:
        engine = create_engine(DATABASE_URI)
        data.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded into the table '{TABLE_NAME}'.")
    except Exception as e:
        logging.error(f"Error loading data into the database: {e}")
        raise


def etl_process(csv_file):
    """
    Main function of the ETL process.

    :param csv_file: Path to the CSV file.
    """
    logging.info("Starting the ETL process...")
    try:
        data = extract_data(csv_file)
        transformed_data = transform_data(data)
        load_data(transformed_data)
        logging.info("ETL process completed successfully.")
    finally:
        logging.info("ETL process has ended.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide the path to the CSV file.")
        sys.exit(1)

    csv_file_path = sys.argv[1]

    if not os.path.exists(csv_file_path):
        print(f"File not found: {csv_file_path}")
    else:
        try:
            etl_process(csv_file_path)
            print("ETL process completed successfully.")
        except ValueError as e:
            print(e)
