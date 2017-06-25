from random import randint
from datetime import datetime
import uuid
import time


'''
Class used to hold data for TaskDoc. Methods can be used inside tests. 
'''
class TaskDocData(object):
    valid_top_level_keys = ["type", "interval", "time", "filter"]
    valid_filter_keys = ["direction", "unit", "unit_count", "type"]
    supported_task_type = ["close", "forcemerge", "deleteindices"]
    supported_task_options = {"forcemerge": ["max_num_segments"]}
    supported_task_interval = ["daily", "monthly"]
    supported_filter_type = ["filter_by_age",]
    supported_filter_direction = ["older", "younger"]
    supported_filter_unit = ["days"]

    def build_valid_random_data(self):
        data = {}
        data["time"] = time.strftime('%H:%M:%S')
        random_index = randint(0,100) % len(self.supported_task_type)
        data["type"] =  self.supported_task_type[random_index]
        random_index = randint(0,100) % len(self.supported_task_interval)
        data["interval"] = self.supported_task_interval[random_index]

        # Build Options
        if data["type"] in self.supported_task_options.keys():
            options = {}
            random_index = randint(0,100) % len(self.supported_task_options[data["type"]])
            option = self.supported_task_options[data["type"]][random_index]
            option = { option: None }
            data["options"] = option

        # Build Filter
        filters = []
        filter = {}
        random_index = randint(0,100) % len(self.supported_filter_type)
        filter["type"] = self.supported_filter_type[random_index]
        random_index = randint(0,100) % len(self.supported_filter_direction)
        filter["direction"] = self.supported_filter_direction[random_index]
        random_index = randint(0,100) % len(self.supported_filter_unit)
        filter["unit"] = self.supported_filter_unit[random_index]
        filter["unit_count"] = randint(0,50)
        filters.append(filter)
        data["filters"] = filters

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

if __name__ == '__main__':
    t = TaskDocData()
    print(t.build_valid_random_data())
