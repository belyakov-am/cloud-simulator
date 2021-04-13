import pathlib


SRC_DIR = pathlib.Path(__file__).parent.parent.absolute()
LOGS_DIR = str(SRC_DIR / "logs")

RESOURCES_DIR = SRC_DIR / "resources"
VM_TYPES = str(RESOURCES_DIR / "vms.json")
