import elasticsearch
import curator
from curator.actions import ForceMerge
from datetime import datetime, timedelta
from multiprocessing import Process
import time

# Managerc specific indicies.
TASKS_INDEX = "managerc-tasks"
LOGS_INDEX = "managerc-logs"

# ElasticSearch queries
QUERY_ALL = {"query": {"match_all": {}}}

class ManagerCException(Exception):
    def __init__(self, message):
        self.message = message

class ManagerC(object):
    def __init__(self, es_client):
        self.debug_counter = 0
        self.es_client = es_client
        self.processes = {}
        self._ilo = None
        self.actions_map = {
            "forcemerge": ForceMerge
        }
        # This will be populated via the ilo setter.
        self.filters_map = {}

    @property
    def ilo(self):
        return self._ilo

    @ilo.setter
    def ilo(self, ilo):
        # Set self.ilo to passed in ilo instance
        self._ilo = ilo

        # update filter maps to reflect current instance of IndexList
        self.filters_map["filter_by_age"] = self._ilo.filter_by_age

    def _get_tasks(self):
        self.debug_counter += 1
        print(self.debug_counter)

        search_results = self.es_client.search(index=TASKS_INDEX, doc_type="task", body=QUERY_ALL)
        print("Search Results: {}".format(search_results))
        if search_results['hits']['total'] < 1:
            raise ManagerCException(message="ManagerC could find any tasks")

        docs = [h['_source'] for h in search_results['hits']['hits']]

        return docs

    def _should_run_task(self, task):

        # Is task currently executing?
        if task["id"] in self.processes:
            print("Task is currently being executed")
            return

        # Get current time
        current_time = datetime.utcnow()
        print("Current Time: {}".format(current_time))

        # TODO: Fix this: Hardcoded string will break if day is provided for monthly interval
        target_time = datetime.strptime(task["time"], "%H:%M:%S")
        target_time = target_time.replace(day=current_time.day, year=current_time.year, month=current_time.month)
        print("Target Time: {}".format(target_time))

        # If task has been ran...

        if "last_ran" in task and task["last_ran"] is not None:
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

    def _run_task(self, task):
        es = elasticsearch.Elasticsearch(hosts=["localhost"])
        # Create IndexList for duration of this function call.
        # Requires this function to always be accessed synchronously.
        self.ilo = curator.IndexList(es)

        # Find task type and lookup in actions_map
        try:
            action = self.actions_map[task["type"]]
        except LookupError:
            print("Could not find type mapping")
            return

        # Find filters in filter map
        for filt in task["filters"]:

            # Get the appropriate filter function via the filters_map
            filter_func = self.filters_map[filt["type"]]

            # filter out type from filter
            params = {k: filt[k] for k in filt if k != "type"}
            filter_func(**params)

        action = action(self.ilo, **task["options"])

        name = "{}-{}".format(task["type"], task["id"])
        p = Process(target=self._run_task_threadable, args=(action,task), name=name)
        self.processes[task["id"]] = p

        p.start()

        return

    def _run_task_threadable(self, action, task):

        action.do_action()

        # Update last_ran
        current_time = datetime.utcnow().strftime('%d:%H:%M:%S')
        task["last_ran"] = current_time

        # Update record
        es = elasticsearch.Elasticsearch(hosts=["localhost"])
        print("task: {}".format(task))
        es.index(index=TASKS_INDEX, doc_type="task", id=task["id"], body=task)

    def start(self):

        while True:

            # Remove stopped processes
            removable=[]
            for process in self.processes:
                if not self.processes[process].is_alive():
                    removable.append(process)

            print("Removable {}".format(removable))
            for removable_process in removable:
                print("Removing process {} from list".format(removable_process))
                del self.processes[removable_process]
                removable.remove(removable_process)

            # Get tasks from ES:
            tasks = self._get_tasks()
            print(tasks)

            # Get list of runnable tasks
            runnables = [task for task in tasks if self._should_run_task(task)]
            print("Runnables {}".format(runnables))

            # Run tasks
            for task in runnables:
                self._run_task(task)

            print(self.processes)

            # Sleep
            time.sleep(1)

if __name__ == "__main__":
    # ES Client
    es = elasticsearch.Elasticsearch(hosts=["localhost"])

    # New Manager
    m = ManagerC(es)
    m.start()








