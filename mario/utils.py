import os
from datetime import datetime
import logging
logger = logging.getLogger(__name__)


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


def gzip_file(input_path, output_path=None):
    import gzip
    import shutil
    if output_path is None:
        output_path = input_path + ".gz"
    with open(input_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return output_path


def decompress_gzip_file(input_path, output_path=None):
    import gzip
    import os
    import shutil
    if not input_path.endswith(".gz"):
        raise ValueError(f"Input path must end with '.gz': {input_path}")

    if output_path is None:
        output_path = input_path.removesuffix(".gz")

    # Create output directory if not exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        logger.warning(f"Output file already exists and will be overwritten: {output_path}")

    with gzip.open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    return output_path