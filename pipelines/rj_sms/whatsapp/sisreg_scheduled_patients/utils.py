from datetime import datetime
from pathlib import Path
import re
import shutil
from pipelines.utils.utils import log

def create_partitions(data_path: str, partition_directory: str):
    data_path = Path(data_path)
    partition_directory = Path(partition_directory)
    files = data_path.glob("*.csv")

    for file_name in files:
        date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        ano_particao = parsed_date.strftime("%Y")
        mes_particao = parsed_date.strftime("%m")
        data_particao = parsed_date.strftime("%Y-%m-%d")

        output_directory = (
            partition_directory
            / f"ano_particao={int(ano_particao)}"
            / f"mes_particao={int(mes_particao)}"
            / f"data_particao={data_particao}"
        )
        output_directory.mkdir(parents=True, exist_ok=True)

        # Copy file to partition directory
        shutil.copy(file_name, output_directory)

