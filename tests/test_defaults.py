from managerc import IndexListParamsException, IndexListParams
import pytest
from unittest.mock import Mock
import elasticsearch
import curator

from falcon import testing
import managerc.managerc_uwsgi

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
        ES = Mock(return_value=mock_es_cat_client)
        monkeypatch.setattr('elasticsearch.client.CatClient', ES)
        mock_es_cat_client.indices.return_value = {"test": "ok"}

        # Simulate client no params
        result = client.simulate_get(path='/indexList')
        assert result.text == '{"test": "ok"}'

        # Simulate client bad params
        params = {"format": "not_supported"}
        result = client.simulate_get(path='/indexList', params=params)
        assert result.status_code == 400

        # Simulate client good params
        params = {"format": "json", "bytes": "k"}
        result = client.simulate_get(path='/indexList', params=params)
        assert result.status_code == 200



