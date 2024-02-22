import json
import csv

# Define file paths
json_file = "users.json"
csv_file = "users.csv"

# Read JSON data from file
with open(json_file, "r") as f:
    data = json.load(f)

# Define CSV headers
csv_headers = ["account_id", "permissions", "user_name"]

# Open CSV file for writing
with open(csv_file, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
    writer.writeheader()

    # Write each data item as a CSV row
    for item in data:
        writer.writerow(item)

print("CSV file created successfully!")
