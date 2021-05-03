import pathlib


SRC_DIR = pathlib.Path(__file__).parent.parent.absolute()
LOGS_DIR = str(SRC_DIR / "logs")

RESOURCES_DIR = SRC_DIR / "resources"
VM_TYPES = str(RESOURCES_DIR / "vms.json")

# Indicates time required for VM manager to provision VM. For
# simplicity, it is assumed that each VM requires same time to
# be provisioned.
# Declared in seconds.
VM_PROVISION_DELAY: int = 120

ITER_NUMBER = 3
