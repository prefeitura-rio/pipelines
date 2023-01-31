# -*- coding: utf-8 -*-
"""
Utilities for the flooding notification pipeline.
"""
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
import smtplib

import fiona
import geopandas as gpd
from shapely.geometry import Point

fiona.supported_drivers["KML"] = "rw"


def get_circle(
    latitude: float,
    longitude: float,
    radius: float,
    fname: str = None,
) -> gpd.GeoDataFrame:
    """
    Get a circle geometry from a point and a radius.

    Args:
        latitude: Latitude of the point.
        longitude: Longitude of the point.
        radius: Radius of the circle in meters.
        fname: File name to save the circle geometry in KML (if None, the geometry is not saved).

    Returns:
        GeoDataFrame with the circle geometry.
    """
    center = Point(latitude, longitude)
    dataframe = gpd.GeoDataFrame(geometry=gpd.GeoSeries(center).buffer(radius / 111139))
    if fname is not None:
        fname = Path(fname).with_suffix(".kml")
        dataframe.to_file(fname, driver="KML")
    return dataframe


def send_email(
    from_address: str,
    to_address: str,
    subject: str,
    body: str,
    smtp_server: str,
    smtp_port: int,
    smtp_username: str = None,
    smtp_password: str = None,
    tls: bool = True,
    attachment: str = None,
):
    """
    Sends an email, optionally with an attachment.
    """
    message = MIMEMultipart()
    message["From"] = from_address
    message["To"] = to_address
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    if attachment is not None:
        with open(attachment, "rb") as file:
            payload = MIMEBase("application", "octate-stream")
            payload.set_payload(file.read())
        encoders.encode_base64(payload)
        payload.add_header("Content-Disposition", f"attachment; filename={attachment}")
        message.attach(payload)
    session = smtplib.SMTP(smtp_server, smtp_port)
    if tls:
        session.starttls()
    if smtp_username is None:
        smtp_username = from_address
    if smtp_password is not None:
        session.login(smtp_username, smtp_password)
    text = message.as_string()
    session.sendmail(from_address, to_address, text)
    session.quit()
