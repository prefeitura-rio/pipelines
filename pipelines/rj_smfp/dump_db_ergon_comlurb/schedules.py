# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple


#####################################
#
# Ergon Schedules
#
#####################################

ergon_queries = {
    "cargo": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                cargo,
                nome,
                categoria,
                subcategoria,
                dt_extincao,
                controle_vaga,
                por_refer,
                e_aglutinador,
                cargo_aglut,
                tipo_cargo,
                cargo_funcao,
                escolaridade,
                flex_campo_01,
                flex_campo_02,
                flex_campo_03,
                flex_campo_04,
                flex_campo_05,
                flex_campo_06,
                flex_campo_07,
                flex_campo_08,
                flex_campo_09,
                flex_campo_10,
                flex_campo_11,
                flex_campo_12,
                flex_campo_13,
                flex_campo_14,
                flex_campo_15,
                flex_campo_16,
                flex_campo_17,
                flex_campo_18,
                flex_campo_19,
                flex_campo_20,
                flex_campo_21,
                flex_campo_22,
                flex_campo_23,
                flex_campo_24,
                flex_campo_25,
                flex_campo_26,
                flex_campo_27,
                flex_campo_28,
                flex_campo_29,
                flex_campo_30,
                flex_campo_31,
                flex_campo_32,
                flex_campo_33,
                flex_campo_34,
                flex_campo_35,
                flex_campo_36,
                flex_campo_37,
                flex_campo_38,
                flex_campo_39,
                flex_campo_40,
                flex_campo_41,
                flex_campo_42,
                flex_campo_43,
                flex_campo_44,
                flex_campo_45,
                flex_campo_46,
                flex_campo_47,
                flex_campo_48,
                flex_campo_49,
                flex_campo_50,
                pontpubl,
                flex_campo_51,
                flex_campo_52,
                flex_campo_53,
                flex_campo_54,
                flex_campo_55,
                dt_controle_acum,
                pontlei,
                dt_inicio_contr_vaga,
                id_reg
            FROM   ergon.cargos_; 
        """
    },
}


ergon_clocks = generate_dump_db_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 7, 12, 17, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="P25",
    db_host="10.70.6.26",
    db_port="1521",
    db_type="oracle",
    dataset_id="recursos_humanos_ergon_comlurb",
    vault_secret_path="ergon-comlurb",
    table_parameters=ergon_queries,
)

ergon_comlurb_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
