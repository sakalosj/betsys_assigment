import pytest
from psycopg2._psycopg import Notify

from watch_status import event_listener
from watch_status.db import get_connection, set_active_schema, update_column


@pytest.fixture(params=[
    pytest.param(None, id='no_notify'),
    pytest.param(
        '{"table" : "betsys_records", "data" : {"id":1,"data":"testdata6","status":23,"logged_at":"2019-10-02T07:06:09.755312+00:00"}}',
        id='correct_notify'),
]
)
def notifies_mock(mocker, request):
    if request.param is None:
        return []
    else:
        notify_mock = mocker.Mock(spec=Notify)
        notify_mock.payload = request.param
        return [notify_mock]


def test_event_listener_listen_called(mocker):
    mock_select = mocker.patch('select.select')
    mock_select.side_effect = [([], [], []), 1, Exception('end loop')]
    conn_mock = mocker.Mock()
    conn_mock.notifies = [mocker.MagicMock()]
    json_loads_mock = mocker.patch('json.loads')
    payload_dict = {'table': 'betsys_records', 'data': {'id': 1, 'data': 'testdata6', 'status': 23,
                                                        'logged_at': '2'}}
    json_loads_mock.return_value = payload_dict
    stable_heap_mock = mocker.Mock()

    with pytest.raises(Exception, match='end loop'):
        event_listener(conn_mock, 'test_channel', stable_heap_mock)

    stable_heap_mock.push.assert_called_once_with(1, payload_dict)


def test_get_connection(mocker):
    psycopg2_connect_mock = mocker.patch('psycopg2.connect')
    set_active_schema_mock = mocker.patch('watch_status.db.set_active_schema')

    get_connection('DSN', 'schema', 'isolation_level')

    psycopg2_connect_mock.assert_called_once_with('DSN')
    psycopg2_connect_mock().set_isolation_level.assert_called_once_with('isolation_level')
    set_active_schema_mock.assert_called_once_with(mocker.ANY, 'schema')


def test_set_active_schema(mocker):
    conn_mock = mocker.MagicMock()

    set_active_schema(conn_mock, 'schema')

    conn_mock.cursor().__enter__.return_value.execute.assert_called_once_with(mocker.ANY, ('schema',))
    conn_mock.commit.assert_called_once_with()


def test_update_column(mocker):
    # sql_mock = mocker.patch('watch_status.db.sql.SQL')
    conn_mock = mocker.MagicMock()

    update_column(conn_mock, 'table', 'column', 'pk_column', 'pk', 'value')

    conn_mock.cursor().__enter__.return_value.execute.assert_called_once_with(mocker.ANY, ('value', 'pk'))
    conn_mock.commit.assert_called_once_with()
