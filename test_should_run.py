from datetime import datetime
from time import sleep

task = { "last_ran": None, "time": "20:47:00" }

def should_run_task(task):

    print("Analyzing Task: {}".format(task))

    # Get current time
    current_time = datetime.utcnow()
    print(current_time)
    target_time = datetime.strptime(task["time"], "%H:%M:%S")
    target_time = target_time.replace(day=current_time.day, year=current_time.year, month=current_time.month)
    # If task has been ran...
    if task["last_ran"]:
        #TODO: Fix this: Hardcoded string will break if day is provided for monthly interval
        last_ran = datetime.strptime(task["last_ran"], "%d:%H:%M:%S")

        # We ran the task at or after our target_time on current day
        print("Comparing last_ran time: {} to target_time: {}".format(last_ran, target_time))
        if (last_ran.day == current_time.day) and last_ran >= target_time:
            print("Task has already been ran today")
            return False

        elif current_time >= target_time:
            print("Task has not been ran today, target_time is now or has passed")
            return True

    # Task has never been ran before, should we?
    if current_time >= target_time:
        print("Comparing last_ran time: {} to target_time: {}".format(current_time, target_time))
        print("Running task for the first time: {}".format(task))
        return True

    print("Task {} not eligible to run.".format(task))
    return False

while True:
    should_run_task(task)
    sleep(.5)
