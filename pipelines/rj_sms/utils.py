# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log
import pandas as pd
import os


@task
def clean_ascii(input_file_path):
    try:
        with open(input_file_path, "r", encoding="utf-8") as input_file:
            text = input_file.read()

            # Remove non-basic ASCII characters
            cleaned_text = "".join(c for c in text if ord(c) < 128)

            if ".json" in input_file_path:
                output_file_path = input_file_path.replace(".json", "_clean.json")
            elif ".csv" in input_file_path:
                output_file_path = input_file_path.replace(".csv", "_clean.csv")

            with open(output_file_path, "w", encoding="utf-8") as output_file:
                output_file.write(cleaned_text)

            log("Cleaning complete. Cleaned text saved.")

            return output_file_path

    except Exception as e:
        log("An error occurred:", e)


@task
def convert_to_parquet(input_file_path: str, schema: str):
    if ".json" in input_file_path:
        try:
            with open(input_file_path, "r") as file:
                json_data = file.read()
                data = eval(json_data)  # Convert JSON string to Python dictionary

                # Assuming the JSON structure is a list of dictionaries
                try:
                    df = pd.DataFrame(data, dtype=schema)
                    log("Dados carregados com schema")
                except:
                    df = pd.DataFrame(data)
                    log("Dados carregados sem schema")

            # TODO: adicionar coluna com a data da carga (_data_carga)

            destination_path = input_file_path.replace(".json", ".parquet")

        except Exception as e:
            log("An error occurred:", e)

    df.to_parquet(destination_path, index=False)
