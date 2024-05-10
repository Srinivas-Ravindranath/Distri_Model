import os


def check_model_parts(parts: list):
    return all(os.path.exists("RnnModel/" + part) for part in parts)
