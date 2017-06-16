from unittest.mock import Mock
import elasticsearch
import pytest
import datetime
import freezegun
from managerc import IndexListParamsException, IndexListParams, TaskException, TaskDoc, TaskDocData, ManagerC

@pytest.fixture()
def mock_task():
    return Mock(spec=TaskDoc)


class TestManagerC():

    def test_should_run_task(self, mock_task):

        current_time = datetime.datetime(year=2017, month=5,
                                         day=5, hour=12, minute=00,
                                         second=00)

        with freezegun.freeze_time(current_time):
            e = elasticsearch.Elasticsearch()
            m = ManagerC(es_client=e)

            # Test: Tasks has never been ran, current_time < target_time.
            # Assert False
            mock_task.last_ran = None
            mock_task.time = "12:01:00"
            result = m._should_run_task(mock_task.__dict__)
            assert result is False

            # Test: Task has never been ran, current_time >= target_time
            # Assert True
            mock_task.last_ran = None
            mock_task.time = "11:59:00"
            result = m._should_run_task(mock_task.__dict__)
            assert result is True

            mock_task.last_ran = None
            mock_task.time = "12:00:00"
            result = m._should_run_task(mock_task.__dict__)
            assert result is True

            # Test: last_ran day == current_time day (we ran this today)
            # Assert False
            mock_task.last_ran = "05:12:00:00"
            mock_task.time = "12:00:00"
            result = m._should_run_task(mock_task.__dict__)
            assert result is False

            # Test: last_ran day < current_day, current_time < target_time
            # Assert False
            mock_task.last_ran = "04:12:00:00"
            mock_task.time = "12:01:00"
            result = m._should_run_task(mock_task.__dict__)
            assert result is False

            # Test: last_ran day < current_day, current_time >= target_time
            # Assert True
            mock_task.last_ran = "04:12:00:00"
            mock_task.time = "11:59:00"
            result = m._should_run_task(mock_task.__dict__)
            assert result is True

    def test_run_task(self):

        e = elasticsearch.Elasticsearch(hosts=["localhost"])
        m = ManagerC(es_client=e)

        task = {"type": "forcemerge", "filters": [{"type": "filter_by_age",
                                                   "source": "creation_date",
                                                   "direction": "older",
                                                   "unit": "days",
                                                   "unit_count": 2,
                                                   }]}

        m._run_task(task)

        assert False