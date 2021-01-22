import os
import sys
import requests

BASE_URL: str = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

CSVS_LIST_FILE: str = "noaa_files_list.txt"

OUT_DIR = os.path.join("raw-data", "noaa_files")


if __name__ == '__main__':
    """
    Download the NOAA storm data csv files. The downloaded files are 
    compressed as .gz files.
    """
    with open(CSVS_LIST_FILE, "r") as f:
        filenames = f.read().split("\n")

    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR)

    for fn in filenames:
        print(f"INFO: Downloading file: {fn}")
        outpath = os.path.join(OUT_DIR, fn)
        if os.path.isfile(outpath):
            print(f"INFO: File {outpath} already exists!")
            continue

        download_url = BASE_URL + fn
        rsp = requests.get(download_url)
        with open(outpath, "wb") as outfile:
            outfile.write(rsp.content)

    print(f"INFO: Downloads done: {len(filenames):02d} files downloaded.")
    sys.exit(0)
