# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: skip-file
import datetime
import json
import os
import pathlib

import git

from pipelines.rj_cor.meteorologia.radar.rain_dashboard.src.utils.general_utils import (
    print_error,
    print_ok,
    print_warning,
)


def print_and_log(
    message: str, log_entry: dict, print_func, verbose: bool = True
) -> dict:
    log_entry["shell_output"] += message + "\n"
    if verbose:
        print_func(message)
    return log_entry


def save_log(log_entry: dict, output_file):
    with open(output_file, "a") as outfile:
        json.dump(log_entry, outfile)
        outfile.write("\n")


def initialize_log_entry(args: dict) -> dict:
    log_entry = {}
    log_entry["parameters_passed"] = args
    log_entry["datetime"] = str(datetime.datetime.now()) + " BRT"
    log_entry["user"] = os.path.expanduser("~")
    repo = git.Repo(search_parent_directories=True)
    log_entry["git_hash"] = repo.head.object.hexsha
    log_entry["shell_output"] = ""

    return log_entry


def get_predict_filepaths_and_log(args: dict, model_name: str) -> dict:
    standard_arguments = [
        "overwrite",
        "personal",
        "verbose",
        "input_test_set_file",
        "n_jobs",
    ]

    model_parameters = [key for key in args.keys() if key not in standard_arguments]

    submodel_folder_name = ""
    for parameter in model_parameters:
        submodel_folder_name += f"{parameter}={args[parameter]}-"
    submodel_folder_name = submodel_folder_name[:-1]
    output_path = (
        args["personal"] * "personal/"
        + f"radar_cal/{args['input_test_set_file'].replace('_test.jsonl','').replace('_val.jsonl','')}/models/{model_name}/{submodel_folder_name}"
    )
    output_log_filepath = pathlib.Path(output_path + "/predict_log.jsonl")
    output_predict_filepath = pathlib.Path(output_path + "/predict.npy")

    log_entry = initialize_log_entry(args)
    log_entry["input_files"] = {"test_set_file": "", "model_file": ""}
    log_entry["output_files"] = {
        "predict": str(output_predict_filepath),
        "log": str(output_log_filepath),
    }

    input_model_filepath = pathlib.Path(f"{output_path}/model.joblib")
    test_set_filepath = pathlib.Path(
        args["personal"] * "personal/"
        + "data/processed/radar_cal/"
        + args["input_test_set_file"]
    )

    log_entry["input_files"]["test_set_file"] = str(test_set_filepath)
    log_entry["input_files"]["model_file"] = str(input_model_filepath)

    if not test_set_filepath.is_file():
        error_message = "Error: Specified test set file does not exist. Terminating..."
        log_entry = print_and_log(error_message, log_entry, print_error)
        save_log(log_entry, output_log_filepath)
        exit()

    if not input_model_filepath.is_file():
        error_message = (
            "Error: Specified input model file does not exist. Terminating..."
        )
        log_entry = print_and_log(error_message, log_entry, print_error)
        save_log(log_entry, output_log_filepath)
        exit()

    if output_predict_filepath.is_file():
        if args["overwrite"]:
            warning_message = f"Warning: overwriting existing prediction file {output_predict_filepath}"
            log_entry = print_and_log(
                warning_message, log_entry, print_warning, args["verbose"]
            )
            output_predict_filepath.unlink()
        else:
            error_message = f"Error: prediction file {output_predict_filepath} already exists. Pass -o to overwrite. Terminating..."
            log_entry = print_and_log(error_message, log_entry, print_error)
            save_log(log_entry, output_log_filepath)
            exit()

    return {
        "output_log_filepath": output_log_filepath,
        "output_predict_filepath": output_predict_filepath,
        "test_set_filepath": test_set_filepath,
        "input_model_filepath": input_model_filepath,
        "log_entry": log_entry,
    }


def get_train_filepaths_and_log(args: dict, model_name: str) -> dict:
    standard_arguments = ["overwrite", "personal", "verbose", "input_file", "n_jobs"]

    model_parameters = [key for key in args.keys() if key not in standard_arguments]

    submodel_folder_name = ""
    for parameter in model_parameters:
        submodel_folder_name += f"{parameter}={args[parameter]}-"
    submodel_folder_name = submodel_folder_name[:-1]
    output_path = (
        args["personal"] * "personal/"
        + f"radar_cal/{args['input_file'].replace('_train.jsonl','')}/models/{model_name}/{submodel_folder_name}"
    )

    output_log_filepath = pathlib.Path(output_path + "/train_log.jsonl")
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    log_entry = initialize_log_entry(args)
    log_entry["input_file"] = ""
    log_entry["output_files"] = {
        "model": "",
        "params": "",
        "log": str(output_log_filepath),
    }

    train_set_filepath = pathlib.Path(
        args["personal"] * "personal/"
        + "data/processed/radar_cal/"
        + args["input_file"]
    )
    log_entry["input_file"] = str(train_set_filepath)

    if not train_set_filepath.is_file():
        error_message = "Error: Specified input file does not exist. Terminating..."
        log_entry["shell_output"] += f"{error_message}\n"
        print_error(error_message)
        with open(output_log_filepath, "a") as outfile:
            json.dump(log_entry, outfile)
            outfile.write("\n")
        exit()

    output_model_filepath = pathlib.Path(output_path + "/model.joblib")
    output_parameters_filepath = pathlib.Path(output_path + "/params.json")

    log_entry["output_files"]["model"] = str(output_model_filepath)
    log_entry["output_files"]["params"] = str(output_parameters_filepath)

    if output_model_filepath.is_file():
        if args["overwrite"]:
            warning_message = (
                f"Warning: overwriting existing model file {output_model_filepath}"
            )
            log_entry["shell_output"] += f"{warning_message}\n"
            print_warning(warning_message, verbose=args["verbose"])
            output_model_filepath.unlink()
        else:
            warning_message = f"Warning: model file {output_model_filepath} already exists. Pass -o to overwrite. Terminating..."
            log_entry["shell_output"] += f"{warning_message}\n"
            print_warning(warning_message)
            with open(output_log_filepath, "a") as outfile:
                json.dump(log_entry, outfile)
                outfile.write("\n")
            exit()

    if output_parameters_filepath.is_file():
        if args["overwrite"]:
            warning_message = f"Warning: overwriting existing parameters file {output_parameters_filepath}"
            log_entry["shell_output"] += f"{warning_message}\n"
            print_warning(warning_message, verbose=args["verbose"])
            output_parameters_filepath.unlink()
        else:
            warning_message = f"Warning: parameters file {output_parameters_filepath} already exists. Pass -o to overwrite."
            log_entry["shell_output"] += f"{warning_message}\n"
            print_warning(warning_message)
            with open(output_log_filepath, "a") as outfile:
                json.dump(log_entry, outfile)
                outfile.write("\n")
            exit()

    parameters_dict = dict([(key, args[key]) for key in model_parameters])
    parameters_dict["model_name"] = model_name

    return {
        "output_log_filepath": output_log_filepath,
        "output_model_filepath": output_model_filepath,
        "output_parameters_filepath": output_parameters_filepath,
        "train_set_filepath": train_set_filepath,
        "log_entry": log_entry,
        "parameters_dict": parameters_dict,
    }
