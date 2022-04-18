###############################################################################


# from datetime import timedelta, datetime
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import IntervalClock
# from pipelines.constants import constants

# tweets_flamengo_schedule = Schedule(
#     clocks=[
#         IntervalClock(
#             interval=timedelta(minutes=5),
#             start_date=datetime(2021, 1, 1),
#             labels=[
#                 constants.K8S_AGENT_LABEL.value,
#             ],
#             parameter_defaults={
#                 "keyword": "flamengo",
#             },
#         ),
#     ]
# )
