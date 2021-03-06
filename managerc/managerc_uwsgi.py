import sys
sys.path.append("../")

import json
import uuid
import elasticsearch
import falcon
import time
from managerc.data import TaskDocData

# Managerc specific indicies.
TASKS_INDEX = "managerc-tasks"
LOGS_INDEX = "managerc-logs"

# ElasticSearch queries
QUERY_ALL = {"query": {"match_all": {}}}

# Supported values for json input to service. This is used mostly for json validation.
SUPPORTED_TASKS = ('close', 'forcemerge', 'deleteindices')
SUPPORTED_TASK_INTERVAL = ('daily', 'monthly')
SUPPORTED_FILTER_TYPES = ('filter_by_age',)
SUPPORTED_FILTER_DIRECTIONS = ('older', 'younger')
SUPPORTED_FILTER_UNITS = ('days',)

'''
Hooks
'''


def validate_task_index(req, resp, resource, params):
    '''
    Used as a hook, will validate necessary indices exist in ES before performing action
    :return: falcon.HTTPBadRequest
    '''

    es = elasticsearch.client.IndicesClient(resource.es_client)

    tasks = es.exists(TASKS_INDEX)
    logs = es.exists(LOGS_INDEX)

    if not all([tasks, logs]):
        msg = "Managerc indices do not exist"
        raise falcon.HTTPInternalServerError(msg)


'''
Resources
'''


class PingResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.body = ('Healthy')

# IndexList
class IndexListParamsException(Exception):
    def __init__(self, message):
        self.message = message


class IndexListParams(object):
    '''
    This class is used to filter out bad params for our IndexListResource. 
    Once instantiated you can pass object.__dict__ to the cat API and 
    be sure that the parameters we are passing are sanitized. 
    '''

    def __init__(self, format="json", index=None, bytes="k", **kwargs):

        if not any(format == key for key in ["json", "yaml", None]):
            raise IndexListParamsException(message="Bad format value provided: {}".format(format))
        self.format = format

        if not any(bytes == key for key in ["b", "k", "m", "g", None]):
            raise IndexListParamsException(message="Bad bytes value provided: {}".format(bytes))
        self.bytes = bytes

        self.index = index


class IndexListResource(object):
    '''
    IndexListResource is a falcon resource for viewing ElasticSearch indicies. Used as a reference for creating
    jobs. 
    '''

    def __init__(self, es_client):
        self.es_client = es_client

    def on_get(self, req, resp):

        params = req.params

        try:
            index_list_params = IndexListParams(**params)
        except IndexListParamsException as e:
            raise falcon.HTTPError(falcon.HTTP_400, 'Bad Request', e.message)

        cat_client = elasticsearch.client.CatClient(self.es_client)
        results = cat_client.indices(**index_list_params.__dict__)
        results = json.dumps(results)

        resp.body = results


# Tasks
class TaskException(Exception):
    def __init__(self, message):
        self.message = message


class TaskDoc(object):
    '''
    Class for representing a valid Task document in Elasticsearch. If instantiation does not fail a valid 
    Task document was created. This class can be used with the __dict__ meta field, passing a dictionary 
    representation of the class to the ElasticSearch client. 
    '''

    def __init__(self, POST_data):
        '''
        Init validates POST data provided by the client along with generating a unique ID for this document. 
        :param POST_data: Deserialized python object created from client POST data. 
        
        Expected POST schema (refer to global variables at top of source for allowed values): 
        {
            "id": "UUID", # generated within this function
            "type": "close",
            "interval": "daily",
            "time": "15:00:00",
            "filter": {
                "type": "age",
                "direction": "older | younger",
                "unit": "days | months",
                "unit_count": 5
            }
        }
        
        '''

        # Generated in this function, not passed int
        self.id = None
        self.last_ran = None

        self.type = None
        self.interval = None
        self.time = None
        self.filter = None

        # Check required top level keys are present
        keys = list(POST_data.keys())

        if not all((k in TaskDocData.valid_top_level_keys) for k in keys):
            raise TaskException(message="POST data is missing a required key(s).")

        # Check required filter keys are present
        keys = list(POST_data["filter"].keys())

        if not all((k in TaskDocData.valid_filter_keys) for k in keys):
            raise TaskException(message="POST data is missing required keys(s) in the filter object")

        # Check top level values are valid
        if (POST_data["type"] not in TaskDocData.supported_task_type) \
                or (POST_data["interval"] not in TaskDocData.supported_task_interval):
            raise TaskException(message="Provided value unknown")

        # Check time format
        try:
            time.strptime(POST_data["time"], '%H:%M:%S')
        except:
            raise TaskException(message="Specified time format incorrect. '%H:%M:%S")

        # Check filter values are valid.
        filter = POST_data["filter"]
        if (filter["type"] not in TaskDocData.supported_filter_type) \
                or (filter["direction"] not in TaskDocData.supported_filter_direction) \
                or (filter["unit"] not in TaskDocData.supported_filter_unit) \
                or (not isinstance(filter["unit_count"], int)):
            raise TaskException(message="Provided value for filter unknown")

        # Generate ID
        id = str(uuid.uuid1())

        # Schema is validated, populate class object.
        self.id = id
        self.type = POST_data["type"]
        self.interval = POST_data["interval"]
        self.time = POST_data["time"]
        self.filter = POST_data["filter"]


class TaskResource(object):
    '''
    TaskResource is a falcon resource for viewing and creating scheduled tasks. These tasks define
    when a curator job is ran. Tasks reside in the managerc-task indexlist at the target elasticsearch cluster
    '''

    def __init__(self, es_client):
        self.es_client = es_client

    def on_get(self, req, resp, id=None):

        # If ID is present query for task matching ID. We use a search here so the search results json
        # is identical in both cases. (Opposed to using a get, which will break 'hits' code lower)
        if id:
            query = {"query": {"match": {"id": id}}}
            search_results = self.es_client.search(index=TASKS_INDEX, doc_type="task", body=query)
        else:
            search_results = self.es_client.search(index=TASKS_INDEX, doc_type="task", body=QUERY_ALL)

        # If not hits, we have no tasks. Return a json message indicating this
        if search_results['hits']['total'] < 1:
            results = json.dumps({'hits': 0})
            resp.body = results
            return

        # Send back array of source documents which define tasks.
        docs = [h['_source'] for h in search_results['hits']['hits']]
        results = json.dumps(docs)

        resp.body = results

    def on_post(self, req, resp):

        # Make sure content_length is greater then 0
        if req.content_length < 1:
            raise falcon.HTTPBadRequest(body="No data sent with POST")

        # Create POST_data dictionary
        data = req.stream.read(req.content_length)
        POST_data = json.loads(data, encoding='utf-8')

        # Instantiate TaskDoc
        try:
            doc = TaskDoc(POST_data)
        except TaskException as e:
            raise falcon.HTTPError(falcon.HTTP_400, 'Bad Request', e.message)

        # Send validated document to managerc-tasks
        try:
            self.es_client.index(index=TASKS_INDEX, doc_type="task", body=doc.__dict__, id=doc.id)
        except:
            raise falcon.HTTPInternalServerError(description=e.message)

        resp_body = json.dumps({"id": doc.id, "status": "success"})
        resp.body = resp_body


def main():
    # WSGI callable
    app = falcon.API()

    # ES Client
    es = elasticsearch.Elasticsearch(hosts=["localhost"])

    # Resources
    ping = PingResource()
    index_list = IndexListResource(es_client=es)
    tasks = TaskResource(es_client=es)

    # Routes
    app.add_route('/ping', ping)
    app.add_route('/indexList', index_list)
    app.add_route('/tasks', tasks)
    app.add_route('/tasks/{id}', tasks)

    return app


if __name__ == "__main__":
    main()

app = main()
