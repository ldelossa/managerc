import elasticsearch
import curator
from curator.actions import ForceMerge
from datetime import datetime, timedelta
import time

# Managerc specific indicies.
TASKS_INDEX = "managerc-tasks"
LOGS_INDEX = "managerc-logs"

# ElasticSearch queries
QUERY_ALL = {"query": {"match_all": {}}}

class ManagerCException(Exception):
    def __init__(self, message):
        self.message = message

class ManagerC():
    def __init__(self, es_client):
        self.es_client = es_client
        self.ilo = curator.IndexList()
        self.actions_map = {
            "forcemerge": ForceMerge
        }
        self.filters_map = {
            "filter_by_age": self.ilo.filter_by_age
        }

    def _get_tasks(self):

        search_results = self.es_client.search(index=TASKS_INDEX, doc_type="task", body=QUERY_ALL)

        if search_results['hits']['total'] < 1:
            raise ManagerCException(message="ManagerC could find any tasks")

        docs = [h['_source'] for h in search_results['hits']['hits']]

        return docs

    def _should_run_task(self, task):

        # Get current time
        current_time = datetime.utcnow()
        print("Current Time: {}".format(current_time))

        # TODO: Fix this: Hardcoded string will break if day is provided for monthly interval
        target_time = datetime.strptime(task["time"], "%H:%M:%S")
        target_time = target_time.replace(day=current_time.day, year=current_time.year, month=current_time.month)
        print("Target Time: {}".format(target_time))

        # If task has been ran...
        if task["last_ran"]:
            last_ran = datetime.strptime(task["last_ran"], "%d:%H:%M:%S")
            print("Last Ran: {}".format(target_time))

            # We ran this task today already.
            print(last_ran.day, current_time.day)
            if last_ran.day == current_time.day:
                return False

            # We do not run task today, should we?
            if current_time >= target_time:
                return True

            # Task did not run today, but target_time > current_time
            return False

        # Task has never been ran before, should we?
        if current_time >= target_time:
            return True

        return False

    def run_task(self, task):
        # Create IndexList for duration of this function call.
        # Requires this function to always be accessed synchronously.
        self.ilo = curator.IndexList()

        # Find task type and lookup in actions_map
        try:
            action = self.actions_map[task["type"]]
        except LookupError:
            print("Could not find type mapping")
            return

        # Find filters in filter map
        for filter in task["filters"]:

            try:
                filter_func = self.filters_map[filter["type"]]
                # filter out type from filter
                params = {k: filter[k] for k in filter if k != "type"}



    def start(self):

        while True:

            # Get tasks from ES:
            tasks = self._get_tasks()

            # Get list of runnable tasks
            runnables = [task for task in tasks if self._should_run_task(task)]

            # Run curator tasks (implement multiprocessing here)

            # Sleep
            time.sleep(.5)

if __name__ == "__main__":
    # ES Client
    es = elasticsearch.Elasticsearch(hosts=["localhost"])

    # New Manager
    m = ManagerC(es)
    m.start()








