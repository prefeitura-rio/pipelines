# -*- coding: utf-8 -*-
import ftplib
import re
from typing import List, Optional


class FTPClient:
    def __init__(
        self,
        hostname: str,
        username: str,
        password: Optional[str] = None,
        port: Optional[int] = 21,
        passive: Optional[bool] = True,
        timeout: Optional[int] = None,
    ):
        """
        Initialize the FTP client.

        Args:
            hostname (str): The hostname of the FTP server.
            username (str): The username to use for the FTP server.
            password (str): The password to use for the FTP server.
            port (int, optional): The port to use for the FTP server. Defaults to 21.
            passive (bool, optional): Whether to use passive mode. Defaults to True.
            timeout (int, optional): The timeout to use for the FTP server. Defaults to None.
        """
        self._hostname = hostname
        self._username = username
        self._password = password
        self._port = port
        self._passive = passive
        self._timeout = timeout
        self._ftp = ftplib.FTP()

    @property
    def ftp(self) -> ftplib.FTP:
        """
        Returns the underlying FTP object.
        """
        return self._ftp

    def connect(self) -> None:
        """
        Connect to the FTP server.
        """
        self._ftp.connect(self._hostname, self._port, self._timeout)
        self._ftp.login(self._username, self._password)
        self._ftp.set_pasv(self._passive)

    def close(self) -> None:
        """
        Close the FTP connection.
        """
        self._ftp.quit()

    def download(self, remote_path: str, local_path: str) -> None:
        """
        Download a file from the FTP server.

        Args:
            remote_path (str): The path to the file on the FTP server.
            local_path (str): The path to the file on the local machine.
        """
        with open(local_path, "wb") as f:
            self._ftp.retrbinary("RETR " + remote_path, f.write)

    def list_files(self, path: str, pattern: str = None) -> List[str]:
        """
        List the files in a directory on the FTP server.

        Args:
            path (str): The path to the directory on the FTP server.
            pattern (str, optional): A regex pattern to filter the files by. Defaults to None.
        """
        files = self._ftp.nlst(path)
        if pattern:
            pattern = re.compile(pattern)
            files = [f for f in files if pattern.match(f)]
        return files

    def upload(self, local_path: str, remote_path: str):  # noqa
        """
        Upload a file to the FTP server.

        Args:
            local_path (str): The path to the file on the local machine.
            remote_path (str): The path to the file on the FTP server.
        """
        raise NotImplementedError("Not implemented as we won't be using this yet.")
