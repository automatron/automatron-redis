from ConfigParser import NoSectionError

DEFAULT_REDIS_HOST = 'localhost'
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DBID = None
DEFAULT_REDIS_PASSWORD = None
DEFAULT_REDIS_CHARSET = None


def build_redis_config(config_file, additional_section=None):
    config = {
        'host': DEFAULT_REDIS_HOST,
        'port': DEFAULT_REDIS_PORT,
        'dbid': DEFAULT_REDIS_DBID,
        'password': DEFAULT_REDIS_PASSWORD,
        'charset': DEFAULT_REDIS_CHARSET,
    }

    try:
        config_section = dict(config_file.items('redis'))
        config.update(config_section)
    except NoSectionError:
        pass

    if additional_section is not None:
        try:
            config_section = dict(config_file.items(additional_section))
            config.update(config_section)
        except NoSectionError:
            pass

    config['port'] = int(config['port'])
    config['dbid'] = int(config['dbid']) if config['dbid'] is not None else None

    return config
