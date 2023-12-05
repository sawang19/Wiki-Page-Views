import re
import bz2
import os
import gzip
import requests

# this version get the daily original file from https://dumps.wikimedia.org/other/pageview_complete
# and split into small .gz files. there is no logic in splitting. only keep the lines complete and write
# to a new file when it reaches the expected size

def get_output_folder(input_file):
    # Use regular expressions to extract the desired portion from the input file name
    match = re.search(r'(\d{8}-user)', input_file)
    if match:
        output_folder = match.group(1)
        output_folder = f"pageviews-{output_folder}"
    else:
        # Handle the case where the pattern is not found
        raise ValueError("Pattern not found in the input file name")

    return output_folder


def download_input_file(base_url, date, target_file):
    # Construct the URL to download the input file
    url = f"{base_url}/{date[0:4]}/{date[0:4] + '-' + date[4:6]}/pageviews-{date}-user.bz2"

    # Download the file
    response = requests.get(url)
    if response.status_code == 200:
        with open(target_file, 'wb') as file:
            file.write(response.content)
    else:
        raise ValueError(f"Failed to download the input file from URL: {url}")


def split_and_compress(input_bz2_file, target_chunk_size, compress_folder):

    # Open the input .bz2 file
    with bz2.BZ2File(input_bz2_file, 'rb') as bz2_file:
        # Initialize variables for tracking the current lines and part number
        current_lines = []
        part_number = 0
        current_chunk_size = 0
        prefix = "en.wikipedia".encode('utf-8')
        for line in bz2_file:
          if line.startswith(prefix):
            current_lines.append(line)
            current_chunk_size += len(line)

            # If adding the current line would exceed the target chunk size, save the part
            if current_chunk_size > target_chunk_size:
                part_filename = f"part-{part_number}"
                with open(part_filename, 'wb') as part_file:
                    part_file.writelines(current_lines)

                # Compress the part using gzip
                compressed_filename = os.path.join(compress_folder, f"part-{part_number}.gz")
                with open(part_filename, 'rb') as input_part, gzip.open(compressed_filename, 'wb') as output_gzip:
                    output_gzip.writelines(input_part)

                # Clean up the original part file
                os.remove(part_filename)

                # Increment the part number and reset the current lines and chunk size
                part_number += 1
                current_lines = []
                current_chunk_size = 0


if __name__ == '__main__':
    for i in range(0, 9):
        num = 20230622 + i
        date = str(num)
        print(date)
        #date = "20230110"  # Replace with your desired date
        base_url = "https://dumps.wikimedia.org/other/pageview_complete"
        target_file = f"pageviews-{date}-user.bz2"

        download_input_file(base_url, date, target_file)

        target_chunk_size = 16 * 1024 * 1024  # Adjust to the desired target chunk size in bytes
        compress_folder = get_output_folder(target_file)  # Get the output folder name from the input file

        os.makedirs(compress_folder, exist_ok=True)

        split_and_compress(target_file, target_chunk_size, compress_folder)
