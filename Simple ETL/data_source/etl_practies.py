import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        car_model = person.find("car_model").text  # Fixed typo here
        year_of_manufacture = float(person.find("year_of_manufacture").text)
        price = float(person.find("price").text)
        fuel = person.find("fuel").text
        row_data = {
            "car_model": car_model,
            "year_of_manufacture": year_of_manufacture,
            "price": price,
            "fuel": fuel
        }
        dataframe = pd.concat([dataframe, pd.DataFrame([row_data])], ignore_index=True)
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    # Process all csv files
    for csvfile in glob.glob("*.csv"):
        temp_data = extract_from_csv(csvfile)
        if not temp_data.empty:
            extracted_data = pd.concat([extracted_data, temp_data], ignore_index=True)

    # Process all json files
    for jsonfile in glob.glob("*.json"):
        temp_data = extract_from_json(jsonfile)
        if not temp_data.empty:
            extracted_data = pd.concat([extracted_data, temp_data], ignore_index=True)

    # Process all xml files
    for xmlfile in glob.glob("*.xml"):
        temp_data = extract_from_xml(xmlfile)
        if not temp_data.empty:
            extracted_data = pd.concat([extracted_data, temp_data], ignore_index=True)

    return extracted_data

# Function to transform the data (rounding the price to 2 decimal places)
def transform(data):
    data['price'] = data['price'].round(2)
    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  # Corrected timestamp format
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
