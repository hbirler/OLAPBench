import json
import os
import xml.etree.ElementTree as ET

# Directory containing the XML files
directory = "/raid0/data/stackoverflow_old"

# Read the schema from the JSON file
schema = {}
types = {}
with open("stackoverflow.dbschema.json") as file:
    schema_raw = json.load(file)
    for table in schema_raw["tables"]:
        schema[table["name"]] = []
        types[table["name"]] = []
        for column in table["columns"]:
            schema[table["name"]].append(column["name"])
            types[table["name"]].append((column["type"], column["name"]))


def xml_to_csv(input_file, output_file):
    # Parse the XML file
    tree = ET.parse(input_file)
    root = tree.getroot()

    # Open CSV file for writing
    table = os.path.splitext(os.path.basename(input_file))[0]
    columns = schema[table]
    print(f"Converting {table} with columns {columns}")
    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        # Iterate through XML rows and write them to the CSV
        for row in root.findall('row'):
            row_data = {col: row.attrib.get(col, None) for col in columns}  # Get attributes or empty string
            data = []
            for type, col in types[table]:
                if "bool" in type and row_data[col] is not None:
                    row_data[col] = "1" if row_data[col] == "True" else "0"
                elif "varchar" in type or "text" in type:
                    if row_data[col] == '':
                        row_data[col] = '""'

                d = None
                if row_data[col] is None:
                    d = ''
                elif "bool" in type and row_data[col] is not None:
                    d = "1" if row_data[col] == "True" else "0"
                elif "varchar" in type or "text" in type:
                    d = f'"{row_data[col].replace('"', '""')}"'
                else:
                    d = row_data[col]

                assert d is not None
                data.append(d)

            csvfile.write(",".join(data) + "\n")


# Process all XML files in the input directory
for filename in os.listdir(directory):
    if filename.endswith('.xml'):
        input_file_path = os.path.join(directory, filename)
        output_file_path = os.path.join(directory, f"{os.path.splitext(filename)[0]}.csv")
        xml_to_csv(input_file_path, output_file_path)

print(f"Conversion completed! CSV files saved to {directory}")
