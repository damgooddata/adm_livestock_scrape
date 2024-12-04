import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import zipfile
import json

# URL of the directory listing
url = "https://pubfs-rma.fpac.usda.gov/pub/References/adm_livestock/2025/"

generate_url_endpoint = 'https://your-wix-site.com/_functions/generateUploadUrl'
response = requests.post(generate_url_endpoint)
upload_data = response.json()
upload_url = upload_data['uploadUrl']

# Send a GET request to the URL
response = requests.get(url)
html = response.text

# Parse the HTML content
soup = BeautifulSoup(html, 'html.parser')

# Extract lines within the <pre> tag and split on <br>
pre_tag = soup.find('pre')
if pre_tag:
    content = pre_tag.decode_contents()
    lines = content.split('<br/>')

# Remove the first blank entry due to `<br><br>` at the start
lines = [line for line in lines if line.strip()]

# Pattern to extract details
pattern = r'^(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2} [AP]M)\s+([\d,]+)\s+<a href="([^"]+)">(.+)</a>$'

# List to store file details
file_list = []

# Process each line
for line in lines:
    line = line.strip()
    if not line or 'To Parent Directory' in line:
        continue

    match = re.match(pattern, line)
    if match:
        date = match.group(1)
        time = match.group(2)
        date_time = f"{date} {time}"
        size = match.group(3).replace(',', '')  # Remove commas from size
        href = match.group(4).lstrip('./')  # Clean up the href if necessary
        file_name = match.group(5)

        # Construct the full download link
        download_link = f"{url}{href}"

        file_list.append({
            'date_time': datetime.strptime(date_time, '%m/%d/%Y %I:%M %p'),  # Convert to datetime for sorting
            'file_name': file_name,
            'download_link': download_link
        })

if file_list:
    most_recent_file = sorted(file_list, key=lambda x: x['date_time'], reverse=True)[0]

    # Print the most recent file info
    print(f"Date/Time: {most_recent_file['date_time'].strftime('%m/%d/%Y %I:%M %p')}")
    print(f"File Name: {most_recent_file['file_name']}")
    print(f"Download Link: {most_recent_file['download_link']}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, most_recent_file['file_name'])

    file_response = requests.get(most_recent_file['download_link'], stream=True)
    with open(file_path, 'wb') as file:
        for chunk in file_response.iter_content(chunk_size=1024):
            file.write(chunk)
    print(f"File downloaded: {file_path}")

    extracted_file_path = None
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(script_dir)
        extracted_file_path = os.path.join(script_dir, zip_ref.namelist()[0])  # Assume single file in ZIP
        print(f"File extracted to: {extracted_file_path}")
    else:
        print("The downloaded file is not a ZIP file.")
        extracted_file_path = file_path

    # Process the extracted file (pipe-delimited text file) and convert to JSON
    if extracted_file_path and extracted_file_path.endswith('.txt'):
        json_file_path = extracted_file_path.replace('.txt', '.json')
        with open(extracted_file_path, 'r') as txt_file:
            headers = txt_file.readline().strip().split('|')  # Get the column headers
            data = [
                dict(zip(headers, line.strip().split('|')))
                for line in txt_file if line.strip()
            ]
        
        # Write the JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"JSON file created: {json_file_path}")

        with open(json_file_path, 'rb') as file:
            headers = {
                'Content-Type': 'application/json',
            }
            params = {
                'filename': 'file.json',
            }
            upload_response = requests.put(upload_url, headers=headers, params=params, data=file)
            upload_response.raise_for_status()

        # Step 3: Confirm the file is uploaded
        if upload_response.status_code == 200:
            print(f'File uploaded successfully.')
        else:
            print(f'Failed to upload file. Status code: {upload_response.status_code}')
else:
    print("No files found.")
