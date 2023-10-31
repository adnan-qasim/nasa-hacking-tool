import json

# Define the input and output file paths
input_file = 'text.txt'
output_file = 'proxies-list-small.json'

# Initialize an empty list to store the floats
proxy_list = []

# Open the input file and read floats line by line
with open(input_file, 'r') as file:
    for line in file:
        proxy = line.strip()  # Remove leading/trailing spaces and newline characters
        proxy_list.append(proxy)

# Open the output file and write the list as JSON
with open(output_file, 'w') as file:
    json.dump(proxy_list, file, indent=4)

print(f"Floats saved to {output_file} as a JSON list.")