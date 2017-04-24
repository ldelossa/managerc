import falcon
import elasticsearch
import curator
import json


class PingResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.body = ('Healthy')


class IndexListParams(object):
    '''
    This class is used to filter out bad params for our IndexListResource. 
    Once instantiated you can pass object.__dict__ to the cat API and 
    be sure that the parameters we are passing are sanitized. 
    '''
    def __init__(self, format="json", index=None, bytes="k", **kwargs):

        print(format, index, bytes)
        if not any(format == key for key in ["json", "yaml", None]):
            print("format exception raised")
            raise Exception
        self.format = format

        if not any(bytes == key for key in [ "b", "k", "m", "g", None]):
            print("bytes exception raised")
            raise Exception

        self.bytes = bytes

        self.index = index


class IndexListResource(object):
    def __init__(self, es_client=None):
        self.es_client = es_client

    def on_get(self, req, resp):

        params = req.params
        print(params)

        index_list_params = IndexListParams(**params)
        print(index_list_params.__dict__)

        cat_client = elasticsearch.client.CatClient(self.es_client)
        results = cat_client.indices(**index_list_params.__dict__)
        results = json.dumps(results)

        resp.body = results


def main():
    # WSGI callable
    app = falcon.API()

    # ES Client
    es = elasticsearch.Elasticsearch(hosts=[
        "35.185.28.10"
    ],
    )

    # Resources
    ping = PingResource()
    index_list = IndexListResource(es_client=es)

    # Routes
    app.add_route('/ping', ping)
    app.add_route('/indexlist', index_list)
    return app

app = main()
