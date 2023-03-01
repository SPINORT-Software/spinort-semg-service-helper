from enum import Enum


class Commands(Enum):
    """
    Commands received by the service.
    Command "name" received from the engine is always excepted to be in all lower-case letters.
    """
    calibration_start = {"name": "calibration_start", }
    calibration_step_start = {"name": "calibration_step_start", }
    calibration_end = {"name": "calibration_end", }
    calibration_pause = {"name": "calibration_pause", }
    treatment_start = {"name": "treatment_start"}
    treatment_one_min_end = {"name": "treatment_one_min_end", }
    treatment_end = {"name": "treatment_end", }
    treatment_start_data_send = {"name": "treatment_start_data_send", }

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
