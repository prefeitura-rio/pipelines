# -*- coding: utf-8 -*-
"""
Tasks for the Systems Bot flow.
"""

from datetime import datetime

from jinja2 import Environment
import pandas as pd
from prefect import task
import telegram
from telegram.utils.helpers import escape_markdown

from pipelines.constants import constants
from pipelines.utils.utils import (
    send_telegram_message,
    smart_split,
)


@task(checkpoint=False)
def get_data(sheet_id: str, sheet_name: str) -> pd.DataFrame:
    """
    Fetches data from Google Drive
    """
    url = "https://docs.google.com/spreadsheets/d/"
    url += f"{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    dataframe = pd.read_csv(url)
    return dataframe


@task(checkpoint=False)
def preprocess_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses data
    """
    dataframe["dt_critica_dt"] = dataframe["dt_critica"].apply(
        lambda x: datetime.strptime(x, "%d/%m/%Y") if isinstance(x, str) else None
    )
    dataframe["dt_iplan_dt"] = dataframe["dt_iplan"].apply(
        lambda x: datetime.strptime(x, "%d/%m/%Y") if isinstance(x, str) else None
    )
    dataframe["texto"] = dataframe.apply(
        lambda x: f"{x['dt_critica']}, {x['nome']} | {x['nome_fantasia']}", 1
    )
    return dataframe.sort_values(by="dt_critica_dt")[
        ["fase", "status", "texto"]
    ].to_dict("records")


@task(checkpoint=False)
def build_message(data: list) -> str:
    """
    Builds the message
    """
    msg = """
    {% macro printconds(data, status, fase) -%}

        {% for d in data -%}
        {% if (d.status == status) and (d.fase == fase) -%}
        - {{d.texto}}
        {% endif %}
        {%- endfor %}
    {% endmacro -%}


    Status dos Sistemas

    {% for f in  fases-%}
    *{{f}} - até {{prazos[f]}}*

    {% for s in  statuses-%}
    *{{s}}*
        {{ printconds(data, s, f) }}
    {%- endfor %}
    {%- endfor %}
    """
    statuses = [
        "Não Iniciado",
        "Em Andamento",
        "Funcionamento Parcial",
        "Funcionamento Pleno",
    ]
    fases = ["Fase 1", "Fase 2", "Fase 3", "Fase 4"]
    prazos = {
        "Fase 1": "05/09",
        "Fase 2": "26/09",
        "Fase 3": "10/10",
        "Fase 4": "07/11",
    }
    return (
        Environment()
        .from_string(msg)
        .render(
            data=data,
            statuses=statuses,
            fases=fases,
            prazos=prazos,
        )
    )


@task(checkpoint=False)
def split_and_send_messages(token: str, group_id: str, message: str) -> None:
    """
    Sends the alerts to the Telegram group.
    """
    messages = smart_split(
        text=message,
        max_length=constants.TELEGRAM_MAX_MESSAGE_LENGTH.value,
        separator="\n",
    )

    for msg in messages:
        if msg != "":
            msg = escape_markdown(msg, version=2)
            send_telegram_message(
                message=msg,
                token=token,
                chat_id=group_id,
                parse_mode=telegram.ParseMode.MARKDOWN_V2,
            )
