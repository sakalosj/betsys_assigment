import pytest
from psycopg2._psycopg import Notify

from watch_status import event_listener


@pytest.mark.parametrize('select_val', ([1, Exception('end loop')],))
@pytest.mark.parametrize('notifies', [
    pytest.param([], id='no_notify'),
    pytest.param(['{"table" : "betsys_records", "data" : {"id":1,"data":"testdata6","status":23,"logged_at":"2019-10-02T07:06:09.755312+00:00"}}'], id='correct_notify'),
    pytest.param(['some_string'], id='incorrect_notify'),
])
def test_event_listener_listen_called(mocker, select_val, notifies):
    mock_select = mocker.patch('select.select')
    mock_select.side_effect = select_val

    mocker.patch('json.loads')
    conn_mock = mocker.Mock()

    notify_mock = mocker.Mock(spec=Notify)
    notify_mock.payload = notifies
    conn_mock.notifies = []
    stable_heap_mock = mocker.Mock()
    with pytest.raises(Exception, match='end loop'):
        event_listener(conn_mock, 'test_channel', stable_heap_mock)
