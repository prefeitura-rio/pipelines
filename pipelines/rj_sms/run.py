# -*- coding: utf-8 -*-
from pipelines.rj_sms.dump_api_vitacare.flows import dump_vitacare_posicao
from pipelines.rj_sms.dump_api_vitai.flows import (
    dump_vitai_posicao,
    dump_vitai_movimentos,
)
from pipelines.utils.utils import run_local
from datetime import date, timedelta


# create a list of dates between start and end date. Dates like YYYY-MM-DD
start_date = date(2023, 2, 1)
end_date = date(2023, 2, 10)
delta = end_date - start_date
dates = []
for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    dates.append(day.strftime("%Y-%m-%d"))

for date in dates:
    print(date)
    run_local(dump_vitai_movimentos, parameters={"date": date})
