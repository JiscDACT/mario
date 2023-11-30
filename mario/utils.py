import os
from datetime import datetime


def append_current_date_to_file_name(file_name: str) -> str:
    date_time = datetime.now().strftime("%Y_%m_%d")
    filename = os.path.splitext(os.path.basename(file_name))[0]
    extension = os.path.splitext(os.path.basename(file_name))[1]
    return filename + '_' + date_time + extension
