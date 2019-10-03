
class Main:
    cfg_file = '../cfg/watch_status.cfg'
    log_file = '/var/log/watch_status.log'
    max_workers = 3
    workers_output_file = 'status_changes.log'

class Db:
    # DSN = 'postgresql://postgres:postgres@192.168.99.101/postgres'
    DSN = 'postgresql://postgres:password@localhost/postgres'
    schema = 'betsys'
    channel = 'events'

class Redis:
    host = '192.168.99.101'
    port = 6379
    db = 0


class cfg:
    """
    class configuration namespace - overriden by cfg file if present
    """
    main = Main
    db = Db
    redis = Redis
