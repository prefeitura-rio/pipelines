# -*- coding: utf-8 -*-
# pylint: disable=R0903
"""
Customizing stuff for the pipelines package.
"""

from datetime import timedelta
from functools import partial
from typing import Callable, Iterable, List, Optional, Set, Union

import prefect
from prefect import task
from prefect.backend.flow_run import FlowRunView, watch_flow_run
from prefect.core.edge import Edge
from prefect.core.flow import Flow
from prefect.core.task import Task
from prefect.engine.result import Result
from prefect.engine.signals import signal_from_state
from prefect.engine.state import State
from prefect.environments import Environment
from prefect.executors import Executor
from prefect.run_configs import RunConfig
from prefect.schedules import Schedule
from prefect.storage import Storage

from pipelines.constants import constants
from pipelines.utils.utils import notify_discord_on_failure, skip_if_running_handler


class CustomFlow(Flow):
    """
    A custom Flow class that implements code ownership in order to make it easier to
    notify people when a FlowRun fails.
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-locals
        self,
        name: str,
        schedule: Schedule = None,
        executor: Executor = None,
        environment: Environment = None,
        run_config: RunConfig = None,
        storage: Storage = None,
        tasks: Iterable[Task] = None,
        edges: Iterable[Edge] = None,
        reference_tasks: Iterable[Task] = None,
        state_handlers: List[Callable] = None,
        validate: bool = None,
        result: Optional[Result] = None,
        terminal_state_handler: Optional[
            Callable[["Flow", State, Set[State]], Optional[State]]
        ] = None,
        code_owners: Optional[List[str]] = None,
        skip_if_running: bool = False,
    ):
        if skip_if_running:
            if state_handlers is None:
                state_handlers = []
            state_handlers.append(skip_if_running_handler)
        super().__init__(
            name=name,
            schedule=schedule,
            executor=executor,
            environment=environment,
            run_config=run_config,
            storage=storage,
            tasks=tasks,
            edges=edges,
            reference_tasks=reference_tasks,
            state_handlers=state_handlers,
            # on_failure=partial(
            #     notify_discord_on_failure,
            #     secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
            #     code_owners=code_owners,
            # ),
            validate=validate,
            result=result,
            terminal_state_handler=terminal_state_handler,
        )


def wait_for_flow_run_with_timeout(timeout: Union[int, timedelta]):
    """
    Builds the `wait_for_flow_run` task with a timeout.

    Example: if you provide `timeout=120`, it would be equivalent to:

    ```python
    @task(timeout=120)
    def wait_for_flow_run(...):
        ...
    ```
    """

    @task(timeout=timeout)
    def wait_for_flow_run(
        flow_run_id: str,
        stream_states: bool = True,
        stream_logs: bool = False,
        raise_final_state: bool = False,
    ) -> "FlowRunView":
        """
        Task to wait for a flow run to finish executing, streaming state and log information

        Args:
            - flow_run_id: The flow run id to wait for
            - stream_states: Stream information about the flow run state changes
            - stream_logs: Stream flow run logs; if `stream_state` is `False` this will be
                ignored
            - raise_final_state: If set, the state of this task will be set to the final
                state of the child flow run on completion.

        Returns:
            FlowRunView: A view of the flow run after completion
        """

        flow_run = FlowRunView.from_flow_run_id(flow_run_id)

        for log in watch_flow_run(
            flow_run_id, stream_states=stream_states, stream_logs=stream_logs
        ):
            message = f"Flow {flow_run.name!r}: {log.message}"
            prefect.context.logger.log(log.level, message)  # pylint: disable=no-member

        # Get the final view of the flow run
        flow_run = flow_run.get_latest()

        if raise_final_state:
            state_signal = signal_from_state(flow_run.state)(
                message=f"{flow_run_id} finished in state {flow_run.state}",
                result=flow_run,
            )
            raise state_signal
        return flow_run

    return wait_for_flow_run
