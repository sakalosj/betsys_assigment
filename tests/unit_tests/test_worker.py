import pytest

from watch_status.worker import worker, log_action


def test_worker(mocker):
    payload_dict = {'table': 'betsys_records', 'data': {'id': 1, 'data': 'testdata6', 'status': 23,
                                                        'logged_at': '2'}}
    conn_mock = mocker.MagicMock()
    update_column_mock = mocker.patch('watch_status.worker.update_column')
    stable_heap_mock = mocker.Mock()
    stable_heap_mock.empty.side_effect = (True, False, Exception('end loop'))
    stable_heap_mock.pop.return_value = payload_dict
    log_action_mock = mocker.patch('watch_status.worker.log_action')

    with pytest.raises(Exception, match='end loop'):
        worker(conn_mock, stable_heap_mock, id, file_name='file_name')

    update_column_mock.assert_called_once_with( mocker.ANY, 'betsys_records', 'logged_at', 'id', 1, mocker.ANY)
    log_action_mock.assert_called_once_with('file_name', mocker.ANY)


def test_log_action(mocker):
    m = mocker.mock_open()
    mocker.patch('watch_status.worker.open', m)

    log_action('file_name', 'data')

    m.assert_called_once_with('file_name', 'a')
    handle = m()
    handle.write.assert_called_once_with('data'+'\n')
