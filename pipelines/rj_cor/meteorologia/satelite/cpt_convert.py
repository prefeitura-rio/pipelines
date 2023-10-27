# -*- coding: utf-8 -*-
"""
cpt_convert explanation
https://geonetcast.wordpress.com/2017/06/02/geonetclass-manipulating-goes-16-data-with-python-part-v/
"""
import colorsys
import numpy as np


def create_color_arrays(lines):
    """
    Create array for each color
    """
    x_array = np.array([])
    r_array = np.array([])
    g_array = np.array([])
    b_array = np.array([])

    color_model = "RGB"

    for line in lines:
        line_split = line.split()
        if line[0] == "#":
            if line_split[-1] == "HSV":
                color_model = "HSV"
                continue

        if line_split[0] == "B" or line_split[0] == "F" or line_split[0] == "N":
            pass
        else:
            x_array = np.append(x_array, float(line_split[0]))
            r_array = np.append(r_array, float(line_split[1]))
            g_array = np.append(g_array, float(line_split[2]))
            b_array = np.append(b_array, float(line_split[3]))
            xtemp = float(line_split[4])
            rtemp = float(line_split[5])
            gtemp = float(line_split[6])
            btemp = float(line_split[7])

        x_array = np.append(x_array, xtemp)
        r_array = np.append(r_array, rtemp)
        g_array = np.append(g_array, gtemp)
        b_array = np.append(b_array, btemp)

    if color_model == "HSV":
        for i in range(r_array.shape[0]):
            r_array[i], g_array[i], b_array[i] = colorsys.hsv_to_rgb(
                r_array[i] / 360.0, g_array[i], b_array[i]
            )

    if color_model == "RGB":
        r_array = r_array / 255.0
        g_array = g_array / 255.0
        b_array = b_array / 255.0
    return x_array, r_array, g_array, b_array


def load_cpt(path):
    """
    Load cpt
    """
    try:
        with open(path, mode="r", encoding="utf-8") as file:
            # coloquei o encoding porque o pylint obriga, pode ser motivo de erros
            lines = file.readlines()
    except FileNotFoundError:
        print("File ", path, "not found")
        return None

    x_array, r_array, g_array, b_array = create_color_arrays(lines)

    x_norm = (x_array - x_array[0]) / (x_array[-1] - x_array[0])

    red = []
    blue = []
    green = []

    for i in range(len(x_array)):
        red.append([x_norm[i], r_array[i], r_array[i]])
        green.append([x_norm[i], g_array[i], g_array[i]])
        blue.append([x_norm[i], b_array[i], b_array[i]])

    color_dict = {"red": red, "green": green, "blue": blue}

    return color_dict
