from runner import worker_loop


def test_worker_loop(mocker):

    conn_mock = mocker.MagickMock()
    stable_heap_mock = mocker.MagickMock()

    worker_loop(conn_mock, stable_heap_mock)

