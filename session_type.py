from enum import Enum


class SessionType(Enum):
    # Session type of the session ID received by the Service
    CALIBRATION = {"name": "calibration"}
    TREATMENT = {"name": "treatment"}

    def __init__(self, value):
        if "name" not in value:
            raise ValueError("Key 'name' needs to be provided")

    @property
    def name(self):
        return self.value["name"]

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)
