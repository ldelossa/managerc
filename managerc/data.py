from random import randint
from datetime import datetime
import uuid


'''
Class used to hold data for TaskDoc. Methods can be used inside tests. 
'''
class TaskDocData(object):
    def __init__(self):
        self.valid_top_level_keys = ["type", "interval", "time", "filter", "id"]
        self.valid_filter_keys = ["direction", "unit", "unit_count", "type"]
        self.supported_task_type = ["close", "forcemerge", "delete"]
        self.supported_task_interval = ["daily", "monthly"]
        self.supported_filter_type = ["age"]
        self.supported_filter_direction = ["older", "younger"]
        self.supported_filter_unit = ["days"]

    def build_valid_random_data(self):
        data = {}
        data["time"] = str(datetime.now())
        data["id"] = str(uuid.uuid1())
        random_index = randint(0,100) % len(self.supported_task_type)
        data["type"] =  self.supported_task_type[random_index]
        random_index = randint(0,100) % len(self.supported_task_interval)
        data["interval"] = self.supported_task_interval[random_index]
        filter = {}
        random_index = randint(0,100) % len(self.supported_filter_type)
        filter["type"] = self.supported_filter_type[random_index]
        random_index = randint(0,100) % len(self.supported_filter_direction)
        filter["direction"] = self.supported_filter_direction[random_index]
        random_index = randint(0,100) % len(self.supported_filter_unit)
        filter["unit"] = self.supported_filter_unit[random_index]
        filter["unit_count"] = randint(0,50)
        data["filter"] = filter

        return data

    def build_invalid_random_data(self):

        # Create random good data
        data = self.build_valid_random_data()

        # Inject random bad data
        random_index = randint(0,100) % len(data.keys())
        random_key = self.valid_top_level_keys[random_index]

        if random_key == "filter":
            random_index = randint(0,100) % len(data["filter"].keys())
            random_filter_key = self.valid_filter_keys[random_index]

            data["filter"][random_filter_key] = "unsupported"
            return data

        data[random_key] = "unsupported"
        return data


