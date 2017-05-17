import pprint

class EsTaskSearchResults():
    def __init__(self):
        self.data = {'took': 8, 'timed_out': False, '_shards': {'total': 5, 'successful': 5, 'failed': 0}, 'hits': {'total': 4, 'max_score': 1.0, 'hits': [{'_index': 'managerc-tasks', '_type': 'task', '_id': 'c3532d1a-339e-11e7-ac83-784f436507cf', '_score': 1.0, '_source': {'type': 'close', 'interval': 'daily', 'time': '15:00:00', 'filter': {'type': 'age', 'direction': 'older', 'unit': 'days', 'unit_count': 5}}}, {'_index': 'managerc-tasks', '_type': 'task', '_id': '<built-in function id>', '_score': 1.0, '_source': {'id': '27556d64-35b6-11e7-bda2-784f436507cf', 'type': 'close', 'interval': 'daily', 'time': '15:00:00', 'filter': {'type': 'age', 'direction': 'older', 'unit': 'days', 'unit_count': 5}}}, {'_index': 'managerc-tasks', '_type': 'task', '_id': 'cc911b52-339f-11e7-9aba-784f436507cf', '_score': 1.0, '_source': {'id': 'cc911b52-339f-11e7-9aba-784f436507cf', 'type': 'close', 'interval': 'daily', 'time': '15:00:00', 'filter': {'type': 'age', 'direction': 'older', 'unit': 'days', 'unit_count': 5}}}, {'_index': 'managerc-tasks', '_type': 'task', '_id': 'a9afa5e8-339f-11e7-bfc9-784f436507cf', '_score': 1.0, '_source': {'type': 'close', 'interval': 'daily', 'time': '15:00:00', 'filter': {'type': 'age', 'direction': 'older', 'unit': 'days', 'unit_count': 5}, 'id': 'a9afa5e8-339f-11e7-bfc9-784f436507cf'}}]}}

    def pretty_print(self):
        pprint.pprint(self.data)

if __name__ == '__main__':
    EsTaskSearchResults().pretty_print()