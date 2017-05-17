import managerc.managerc_uwsgi
import pytest
import elasticsearch
from falcon import testing
from unittest.mock import Mock
from managerc import IndexListParamsException, IndexListParams, TaskException, TaskDoc, TaskDocData

# fixture which simulates a web client
@pytest.fixture()
def client():
    return testing.TestClient(managerc.managerc_uwsgi.main())

@pytest.fixture()
def mock_es():
    return Mock(spec=elasticsearch)

@pytest.fixture()
def mock_es_cat_client():
    return Mock(spec=elasticsearch.client.CatClient)

class TestIndexList():

    def test_params_instantiation(self):

        with pytest.raises(IndexListParamsException):
            i = IndexListParams(format="no_supported", index=None, bytes="k")

        with pytest.raises(IndexListParamsException):
            i = IndexListParams(format="json", index=None, bytes="not_supported")

        i = IndexListParams(format="json", index=None, bytes="k")
        assert isinstance(i, IndexListParams)

        i = IndexListParams(format="json", index="Test", bytes="k")
        assert ((i.format == "json") and (i.index == "Test") and (i.bytes == "k"))

    def test_get(self, monkeypatch, client, mock_es_cat_client):
        # Create mock object in which when called returns our mock_es_cat_client mock
        ES = Mock(return_value=mock_es_cat_client)
        # Via monkeypatch, replace CatClient with ES callable, thus returning our mock to the code in play
        monkeypatch.setattr('elasticsearch.client.CatClient', ES)
        # Patch our cat_client mock to return specific value for testing
        mock_es_cat_client.indices.return_value = {"test": "ok"}

        # Simulate client no params
        result = client.simulate_get(path='/indexList')
        assert result.text == '{"test": "ok"}'

        # Simulate client good params
        params = {"format": "json", "bytes": "k"}
        result = client.simulate_get(path='/indexList', params=params)
        mock_es_cat_client.indices.assert_called()
        assert result.text == '{"test": "ok"}'
        assert result.status_code == 200

        # Simulate client bad params
        params = {"format": "not_supported"}
        result = client.simulate_get(path='/indexList', params=params)
        mock_es_cat_client.indices.assert_called()
        assert result.status_code == 400

class TestTask():

    def test_task_doc_instatiation(self):

        # Test with valid POST_data
        task_doc_data = TaskDocData()
        POST_data = task_doc_data.build_valid_random_data()
        assert TaskDoc(POST_data)

        # Test with invalid POST_data
        task_doc_data = TaskDocData()
        POST_data = task_doc_data.build_invalid_random_data()
        print(POST_data)
        with pytest.raises(TaskException):
            TaskDoc(POST_data)
