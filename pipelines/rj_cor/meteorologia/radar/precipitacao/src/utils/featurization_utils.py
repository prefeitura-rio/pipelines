# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: skip-file
import numpy as np

from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.PointsToFeatures import (
    PointsToFeatures,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.RadarData import (
    RadarData,
)


# Implementation for two operands only for now
def parse_operation(
    p2f: PointsToFeatures, rd: RadarData, operation_descriptor: str
) -> np.array:
    for basic_operation_desc, basic_operation_func in BASIC_OPERATIONS.items():
        if basic_operation_desc in operation_descriptor:
            operand1_desc, operand2_desc = operation_descriptor.split(
                basic_operation_desc
            )

            operand1 = parse_operation(p2f, rd, operand1_desc)
            operand2 = parse_operation(p2f, rd, operand2_desc)

            if basic_operation_desc == "/" and operand2_desc == "beam:rain_bins":
                result = np.where(
                    operand2 == 0,
                    np.divide(operand1, operand2 + 0.0001),
                    np.divide(operand1, operand2),
                )
            else:
                result = basic_operation_func(operand1, operand2)
            return result

    if operation_descriptor == "dist:":
        return p2f.get_dist()
    elif "_nearest" in operation_descriptor:
        k, _ = operation_descriptor.split("_")
        k = int(k)
        _, operation = operation_descriptor.split(":")

        result = p2f.apply_nearest_neighbors(rd, operation, k)

        assert not np.count_nonzero(
            ~np.isfinite(result)
        ), f"Result from {operation_descriptor} contains infinity or NaN in {np.where(~np.isfinite(result))}"
        return result
    elif "beam" in operation_descriptor:
        _, operation_with_param = operation_descriptor.split(":")
        if "," in operation_with_param:
            operation, param = operation_with_param.split(",")
            threshold = float(param)

            result = p2f.apply_beam(rd, operation, threshold)
            assert not np.count_nonzero(
                ~np.isfinite(result)
            ), f"Result from {operation_descriptor} contains infinity or NaN in {np.where(~np.isfinite(result))}"
            return result
        else:
            operation = operation_with_param
            result = p2f.apply_beam(rd, operation)
            assert not np.count_nonzero(
                ~np.isfinite(result)
            ), f"Result from {operation_descriptor} contains infinity or NaN in {np.where(~np.isfinite(result))}"
            return result
    else:
        raise Exception(f"Invalid {operation_descriptor} descriptor.")


BASIC_OPERATIONS = {"+": np.add, "-": np.subtract, "/": np.divide, "*": np.multiply}
