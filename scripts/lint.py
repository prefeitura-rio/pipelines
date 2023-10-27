# -*- coding: utf-8 -*-
from glob import glob
import subprocess


def main():
    """
    Lint all python files in the project.
    """
    files = glob("**/*.py", recursive=True)
    subprocess.run(["flake8", *files])


if __name__ == "__main__":
    main()
