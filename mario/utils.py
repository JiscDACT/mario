import os
from datetime import datetime


def append_current_date_to_file_name(file_name: str) -> str:
    date_time = datetime.now().strftime("%Y_%m_%d")
    filename = os.path.splitext(os.path.basename(file_name))[0]
    extension = os.path.splitext(os.path.basename(file_name))[1]
    return filename + '_' + date_time + extension


def to_snake_case(name: str) -> str:
    """
    Convert a name to lowercase_with_underscores.
    Example: "Academic Year" -> "academic_year".
    """
    import re
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"__+", "_", name).strip("_")
    return name