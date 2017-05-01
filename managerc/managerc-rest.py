import falcon
import elasticsearch
import curator
import json


class PingResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.body = ('Healthy')


'''
IndexList sections, build exception, create params class which will sanitize input, create falcon Resource. 
'''
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
    def __init__(self, es_client=None):
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




def main():
    # WSGI callable
    app = falcon.API()

    # ES Client
    es = elasticsearch.Elasticsearch(hosts=["localhost"])

    # Resources
    ping = PingResource()
    index_list = IndexListResource(es_client=es)

    # Routes
    app.add_route('/ping', ping)
    app.add_route('/indexList', index_list)
    return app

app = main()
