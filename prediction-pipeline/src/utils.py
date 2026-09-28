from pathlib import Path


def create_path(path):
	filepath = Path(path)
	filepath.mkdir(parents=True, exist_ok=True)