import pathlib


def mkdir(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def join_path(path_a, path_b):
    return str(pathlib.Path(path_a.rstrip("/")) / path_b.lstrip("/"))
