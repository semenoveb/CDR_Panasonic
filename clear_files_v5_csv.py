# Record Type = line_l[1] must be len(line_l[1]) == 8
# Start Time = line_l[2]
# Duration of Call, seconds = line_l[3] / 10
# Duration round up in minutes = line_l[3]
# Called Party = line_l[10]
# Calling Party = line_l[11]
# Call Answer Time = line_l[47]
# Call Release Time = line_l[48]
# Forwarded from DN = line_l[64]
# Ingress SIP Endpoint Address = line_l[125]
# Egress SIP Endpoint Address = line_l[126]  # "10.95.49.196", "10.95.49.205", "10.95.49.206"

# #############################################
# FILENAME: osv2-20211220T090301+0300235228.BF
# DEVICE: OpenScapeVoice
# HOSTNAME: osv2
# FILETYPE: BILLING
# VERSION: V9.00.00
#
# CREATE: 2021-20-12T09:03:01.1+0300
# rate:
#   len(line_l[10]) <= 12 # Called Party = line_l[10]
#   len(line_l[10]) > 12
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import math


class CollectFiles:
    """Collects files from folder in one main file"""

    def __init__(self, folder_name, general_file):
        self.folder = Path(folder_name)
        self.general_file = Path(self.folder.parent, general_file)
        print(self.general_file)

        if self.general_file.exists():
            self.general_file.unlink()

    def collected_files(self, file_name):
        with open(file_name) as reader, open(self.general_file, 'a') as writer:
            for line in reader:
                if line.startswith(("FILENAME", "DEVICE", "HOSTNAME", "FILETYPE", "VERSION", "CREATE", "CLOSE")):
                    continue
                writer.write(line)


def clear_file(file_read, file_write):
    header = ["Start_Time", "Rate", "Duration_of_Call 'seconds'", "Duration_Round_UP 'minutes'", "Prise",
              "Called_Party", "Calling_Party", "Call_Answer_Time", "Call_Release_Time",
              "Forwarded from DN", "Ingress SIP Endpoint Address", "Egress SIP Endpoint Address", "\n"]
    rate = None
    try:
        with open(file_read) as reader, open(file_write, 'w') as writer:

            header_line = ','.join(header)
            writer.write(header_line)

            for line in reader:
                if len(line) < 2:
                    continue

                line_l = line.strip().split(",")

                if len(line_l) < 126:
                    continue

                if line_l[3] == '0':
                    continue

                if not line_l[2] or line_l[2] == '0':
                    continue

                if not line_l[1]:
                    continue

                if line_l[1][-1].endswith(('1', '2', '3', '4', '5', '6', '7', '8', '9')):
                    continue

                if not line_l[126]:
                    continue
                elif line_l[126] not in ["10.95.49.196", "10.95.49.205", "10.95.49.206"]:
                    continue

                # print(f"{line_l[0]} -- {len(line_l)}")

                # Install Rate
                # Start with ['08', '8', '7'] -- 2.10
                # Least rate -- 10

                if line_l[10].find('0810') != -1:
                    rate = 10
                    # print(rate)
                elif line_l[10].startswith(('08', '8', '7')):
                    rate = 2.10
                else:
                    rate = 2.10
                    # print(f"Rate in else: {rate}")

                # Duration round up in minutes = math.ceil(float((line_l[3] / 10) / 60))
                line_duration_up = math.ceil(float((int(line_l[3]) / 10) / 60))

                # Create Prise, prise = line_duration_up * rate
                prise = line_duration_up * rate

                new_line = ','.join([line_l[2].split('.')[0], str(rate), str(int(line_l[3]) / 10),
                                     str(line_duration_up), str(prise), line_l[10], line_l[11],
                                     line_l[47].split('.')[0],line_l[48].split('.')[0],line_l[64],
                                     line_l[125], line_l[126], '\n'])

                writer.write(new_line)
    except Exception as err:
        print(line_l)
        print(len(line_l))
        print("Something is wrong in file.")
        print(err)
        exit()


def merge_files(src_file_name, tenants_file_name):
    try:
        finished_file = input("Enter merged file name with extension 'csv': ")
        finished_path = Path(Path().cwd(), finished_file)
        if finished_path.exists():
            finished_path.unlink()
        # clear_table = pd.read_csv(src_file_name, dtype={'Called_Party': 'str'})
        clear_table = pd.read_csv(src_file_name, dtype={'Called_Party': 'str', "Duration_of_Call 'seconds'": 'float',
                                                        'rate': 'float'})
        clear_table.drop(columns=['Unnamed: 12'], inplace=True)
        all_tenants = pd.read_csv(tenants_file_name)
        # all_tenants = pd.read_execl(tenants_file_name)
        general_table = pd.merge(clear_table, all_tenants)
        general_table.to_csv(finished_path)
        # general_table.to_excel(finished_path)
    except Exception as err:
        print("Something is wrong on a way.")
        print(err)

    print()
    print(f"You file here: {finished_path}")


def main():
    current_folder = Path.cwd()
    # tenants_file_name = Path("./Tenants/All_subscribers.xlsx")
    tenants_file_name = Path("./Tenants/All_subscribers_csv.csv")
    while True:

        folder_name = input("Enter path to folder with raw files CDR: ")
        general_file = input("Enter collected file name: ")
        collected_file = CollectFiles(folder_name, general_file)

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(collected_file.collected_files, Path(folder_name).iterdir())

        file_read = collected_file.general_file
        file_write = input("Enter the file name that create CDR formatted rows: ")
        file_write_path = Path(current_folder, file_write)
        clear_file(file_read, file_write_path)
        print("File was cleaned, you could see it in directory.")

        print()
        print("########################################################")
        print("#                                                      #")
        print("#               We merge two files now!!!              #")
        print("#                                                      #")
        print("########################################################")
        print()

        merge_files(file_write_path, tenants_file_name)
        print("Merge is finished")

        s_exit = input("If you want to finish work, press 'q', continue -- any key: ")
        if s_exit == 'q':
            break
    print()
    print("++++++++++++You are awesome. Great work.+++++++++++++++")


if __name__ == "__main__":
    main()
