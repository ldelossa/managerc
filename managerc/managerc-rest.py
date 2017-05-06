import falcon
import elasticsearch
import curator
import json


TASKS_INDEX = "managerc-tasks"
LOGS_INDEX = "managerc-logs"

QUERY_ALL = {"query": {"match_all": {}}}

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

        if not any(bytes == key for key in [ "b", "k", "m", "g", None]):
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


class TaskParamsExecption(Exception):
    def __init__(self, message):
        self.message = message

class TaskPOSTData(object):
    '''
    This class is used to validate json schema (keys). Value validation will occur at the curator API.
    '''
    def __init__(self, POST_data):
        self.type = None
        self.interval = None
        self.time = None
        self.filter = None

        # Check required top level keys are present
        top_level_keys = ["type", "interval", "time", "filter"]
        keys = list(POST_data.keys())

        if not all((k in top_level_keys) for k in keys):
            raise TaskParamsExecption(message="POST data is missing a required key(s).")

        # Check required filter keys are present
        filter_keys = ["direction", "unit", "unit_count"]
        keys = list(POST_data.keys())

        if not all((k in filter_keys) for k in keys):
            raise TaskParamsExecption(message="POST data is missing required keys(s) in the filter object")

        # Schema is validated, populate class object.
        self.type = POST_data["type"]
        self.interval = POST_data["interval"]
        self.time = POST_data["time"]
        self.filter = POST_data["filter"]

class TaskResource(object):
    '''
    TaskResource is a falcon resource for viewing and creating scheduled tasks. These tasks define
    when a curator job is ran. Tasks reside in the managerc-task index at the target elasticsearch cluster
    '''

    def __init__(self, es_client):
        self.es_client = es_client

    def on_get(self, req, resp):

        search_results = self.es_client.search(index=TASKS_INDEX, doc_type="task", body=QUERY_ALL)

        # If not hits, we have no tasks. Return a json message indicating this
        if search_results['hits']['total'] < 1:
            results = json.dumps({'hits': 0})
            resp.body = results
            return

        # Send back array of source documents which define tasks.
        docs = [ h['_source'] for h in search_results['hits']['hits'] ]
        results = json.dumps(docs)

        resp.body = results


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
    return app

app = main()
