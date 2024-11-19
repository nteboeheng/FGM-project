from dataclasses import dataclass
import sys
import math
import os
import re
from dotenv import load_dotenv
load_dotenv()

VERSION = "1.0"

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
DAYS_IN_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
FILEHDR_NORMAL = "HDZF R EDI 12440192 -14161 RRRRRRRRRRRRRRRR"
FILEHDR_DNT = "HDZF R EDI 12440192 -14161 DRRRRRRRRRRRRRRR"

class ProgOptions:
    """Class to store program options and settings."""
    def __init__(self):
        self.filename = None
        self.stationname = None
        self.baseline_fpath = None
        self.imfv_header = None
        self.start_minute = 0
        self.stop_minute = 1439
        self.d_nt = 0
        self.scale_factor = 150
        self.lH, self.lD, self.lZ, self.lF = [], [], [], []
        self.H0, self.D0, self.Z0 = 0.0, 0.0, 0.0
        self.year = 0
        self.dayno = 0
        self.nr_minutes = 0

@dataclass
class MinIOConfig:
    """Configuration class to store the connection details to access a MINIO S3 object store

    :param url: The full URL to access the MINIO S3 API
    :type url: str
    :param bucket: The MINIO S3 bucket where objects are stored to be accessed
    :type bucket: str
    :param access_key: The security access key with sufficient access rights
    :type access_key: str
    :param secret_key: The security access key with sufficient access rights
    :type secret_key: str
    :param secure: Is secure transfers used?
    :type secure: bool
    """
    url: str
    bucket: str
    access_key: str
    secret_key: str
    secure: bool

def monday(year, dayno):
    """Convert day-of-year to month and day."""
    DAYS_IN_MONTH[2] = 29 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 28
    month_idx = 1
    while dayno > DAYS_IN_MONTH[month_idx]:
        dayno -= DAYS_IN_MONTH[month_idx]
        month_idx += 1
    return month_idx, dayno

def read_baselines(opt):
    """Read baseline values for H, D, and Z from a file with comments after each value."""
    try:
        with open(opt.baseline_fpath, "rt") as f:
            # Read each line, split by '#', take the first part, strip whitespace, and convert to float
            opt.H0 = float(f.readline().split('#')[0].strip())
            opt.D0 = float(f.readline().split('#')[0].strip())
            opt.Z0 = float(f.readline().split('#')[0].strip())
    except Exception as e:
        sys.exit(f"Error reading baseline file: {e}")

def format_data_line(data_line):
    """Format a data line to ensure proper spacing between values using regex."""
    # Handle cases where values are concatenated (e.g., -0.24483-2.49756)
    formatted_line = re.sub(r'(\d+\.\d+)(-?\d+\.\d+)', r'\1 \2', data_line)
    # Handle cases where multiple negative numbers may be concatenated
    formatted_line = re.sub(r'(\-?\d+\.\d+)(\-?\d+\.\d+)', r'\1 \2', formatted_line)
    return formatted_line

def read_convert_raw(opt):
    """Read raw geomagnetic data, apply baselines, and scale."""
    try:
        opt.lH = [0] * opt.nr_minutes
        opt.lD = [0] * opt.nr_minutes
        opt.lZ = [0] * opt.nr_minutes
        opt.lF = [0] * opt.nr_minutes
        with open(opt.filename, "rt") as f:
            Hscale = opt.scale_factor
            Zscale = -opt.scale_factor
            index = 0
            for line in f:
                line = line.strip()
                if len(line) <= 15:
                    opt.year, opt.dayno, index = map(int, line.split())
                elif len(line) > 15:
                    # Format the line to ensure proper spacing
                    line = format_data_line(line)
                    # Parse H, D, and Z values from the formatted line
                    Hraw, Draw, Zraw, *rest = map(float, line.split()[:3])
                    Htmp = Hraw * Hscale + opt.H0
                    opt.lH[index] = int(Htmp * 10)
                    Dscale = (opt.scale_factor * 3438.0) / Htmp if opt.d_nt == 0 else opt.scale_factor
                    opt.lD[index] = int((Draw * Dscale + opt.D0) * (10 if opt.d_nt else 100))
                    Ztmp = Zraw * Zscale + opt.Z0
                    opt.lZ[index] = int(Ztmp * 10)
                    opt.lF[index] = int(math.sqrt(Htmp**2 + Ztmp**2) * 10) if Htmp and Ztmp else 999999
                    index += 1
    except Exception as e:
        sys.exit(f"Error reading raw data file: {e}")

def dump_data(opt):
    """Write processed data to IMFV1.22 format file."""
    mnth, dom = monday(opt.year, opt.dayno)
    output_file = f"output/{opt.stationname}{opt.dayno:03}{opt.year % 100:02}_{opt.start_minute:04}.fg"
    os.makedirs("output", exist_ok=True)
    with open(output_file, "wt") as out_dataf:
        out_dataf.write(
            f"{opt.stationname.upper()} {MONTHS[mnth - 1]}{dom:02}{opt.year % 100} "
            f"{opt.dayno} {opt.start_minute} {opt.imfv_header}\n"
        )
        for i in range(opt.start_minute, opt.stop_minute + 1, 2):
            out_dataf.write(f"{opt.lH[i]:7} {opt.lD[i]:7} {opt.lZ[i]:7} {opt.lF[i]:6}")
            if i + 1 <= opt.stop_minute:
                out_dataf.write(f"  {opt.lH[i + 1]:7} {opt.lD[i + 1]:7} {opt.lZ[i + 1]:7} {opt.lF[i + 1]:6}\n")

def run_conversion(filename, stationname, baseline_fpath, start_minute=0, stop_minute=1439, d_unit=0, scale_factor=40):
    """
    Run the data conversion process with the specified parameters.
    
    :param filename: Path to the raw data file
    :param stationname: Unique station ID
    :param baseline_fpath: Path to the baseline values file
    :param start_minute: Start minute of the data block (default: 0)
    :param stop_minute: Stop minute of the data block (default: 1439)
    :param d_unit: Specify D unit, default in arc minutes (default: 0)
    :param scale_factor: Scale factor in nT/V (default: 150)
    """
    options = ProgOptions()
    options.filename = filename
    options.stationname = stationname
    options.baseline_fpath = baseline_fpath
    options.imfv_header = FILEHDR_NORMAL if d_unit == 0 else FILEHDR_DNT
    options.start_minute = start_minute
    options.stop_minute = stop_minute
    options.d_nt = d_unit
    options.scale_factor = scale_factor
    options.nr_minutes = options.stop_minute - options.start_minute + 1

    read_baselines(options)
    read_convert_raw(options)
    dump_data(options)
    print(f"Data processing complete. Output saved in 'output/{options.stationname}'")

def extract_12_min_block(input_file, start_minute, block_size=12):
    """
    Extract a 12-minute block of data along with its header.

    :param input_file: Path to the raw data file.
    :param start_minute: The starting minute for the 12-minute block.
    :param block_size: Number of rows to extract (default: 12).
    :return: A tuple containing the header and the block of raw data.
    """
    header = None
    block_data = []
    current_minute = 0  # Tracks the current time from the header

    with open(input_file, "r") as infile:
        for line in infile:
            line = line.strip()

            # Detect header (e.g., 2014 174 0060)
            if len(line.split()) == 3 and all(part.isdigit() for part in line.split()):
                header = line
                current_minute = int(header.split()[2])  # Timestamp in header
                continue

            # Skip rows until the target start_minute
            if current_minute < start_minute:
                current_minute += 1
                continue

            # Collect rows for the 12-minute block
            if current_minute >= start_minute and len(block_data) < block_size:
                block_data.append(line)
                current_minute += 1

            # Stop collecting once the block size is met
            if len(block_data) == block_size:
                break

    # Ensure enough rows were collected
    if len(block_data) < block_size:
        raise ValueError(f"Insufficient data: Only {len(block_data)} rows found for start_minute {start_minute}")

    return header, block_data


def process_and_save_12_min_block(opt, header, raw_block, output_file):
    """
    Process a 12-minute block of data and save it using the dump_data function.

    :param opt: Program options with baselines and scale factors.
    :param header: The header corresponding to the block.
    :param raw_block: The raw 12-minute data block.
    :param output_file: Path to save the processed block.
    """
    opt.lH = []
    opt.lD = []
    opt.lZ = []
    opt.lF = []

    # Parse the header
    opt.year, opt.dayno, start_minute = map(int, header.split())

    # Process raw block
    Hscale = opt.scale_factor
    Zscale = -opt.scale_factor
    for index, line in enumerate(raw_block):
        # Format the line to ensure proper spacing
        line = format_data_line(line)
        # Parse H, D, and Z values from the formatted line
        Hraw, Draw, Zraw, *rest = map(float, line.split()[:3])
        Htmp = Hraw * Hscale + opt.H0
        Dscale = (opt.scale_factor * 3438.0) / Htmp if opt.d_nt == 0 else opt.scale_factor
        Dtmp = Draw * Dscale + opt.D0
        Ztmp = Zraw * Zscale + opt.Z0
        Ftmp = math.sqrt(Htmp**2 + Ztmp**2)

        opt.lH.append(int(Htmp * 10))
        opt.lD.append(int(Dtmp * (10 if opt.d_nt else 100)))
        opt.lZ.append(int(Ztmp * 10))
        opt.lF.append(int(Ftmp * 10) if Htmp and Ztmp else 999999)

    # Update start and stop minute
    opt.start_minute = start_minute
    opt.stop_minute = start_minute + len(raw_block) - 1

    # Save to file
    dump_data(opt)
    print(f"Processed 12-minute block saved to: {output_file}")

if __name__ == "__main__":
    # run_conversion(
    #     filename="data/sample_data.txt",
    #     stationname="HER1",
    #     baseline_fpath="data/baselines.txt",
    #     start_minute=0,
    #     stop_minute=1439,
    #     d_unit=0,
    #     scale_factor=40
    # )
    options = ProgOptions()
    options.filename = "data/sample_data.txt"
    options.baseline_fpath = "data/baselines.txt"
    options.stationname = "HER"
    options.scale_factor = 40

    # Read baselines
    read_baselines(options)

    # Extract and process a 12-minute block
    start_minute = 12  # Change as needed for subsequent blocks
    try:
        header, raw_block = extract_12_min_block(options.filename, start_minute)
        process_and_save_12_min_block(options, header, raw_block, "output/12min_block.fg")
    except ValueError as e:
        print(f"Error: {e}")
