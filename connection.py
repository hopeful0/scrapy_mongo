import six

from scrapy.utils.misc import load_object

from . import defaults


# Shortcut maps 'setting name' -> 'parmater name'.
SETTINGS_PARAMS_MAP = {
    'MONGO_URL': 'url',
    'MONGO_HOST': 'host',
    'MONGO_PORT': 'port',
    # 'MONGO_ENCODING': 'encoding',
    'MONGO_DATABASE': 'database',
    'MONGO_USERNAME': 'username',
    'MONGO_PASSWORD': 'password'
}


def get_mongo_from_settings(settings):
    """Returns a redis client instance from given Scrapy settings object.

    This function uses ``get_client`` to instantiate the client and uses
    ``defaults.REDIS_PARAMS`` global as defaults values for the parameters. You
    can override them using the ``REDIS_PARAMS`` setting.

    Parameters
    ----------
    settings : Settings
        A scrapy settings object. See the supported settings below.

    Returns
    -------
    server
        Redis client instance.

    Other Parameters
    ----------------
    REDIS_URL : str, optional
        Server connection URL.
    REDIS_HOST : str, optional
        Server host.
    REDIS_PORT : str, optional
        Server port.
    REDIS_ENCODING : str, optional
        Data encoding.
    REDIS_PARAMS : dict, optional
        Additional client parameters.

    """
    params = defaults.MONGO_PARAMS.copy()
    params.update(settings.getdict('MONGO_PARAMS'))
    # XXX: Deprecate REDIS_* settings.
    for source, dest in SETTINGS_PARAMS_MAP.items():
        val = settings.get(source)
        if val:
            params[dest] = val

    # Allow ``redis_cls`` to be a path to a class.
    if isinstance(params.get('mongo_cls'), six.string_types):
        params['mongo_cls'] = load_object(params['mongo_cls'])

    return get_mongo(**params)


# Backwards compatible alias.
from_settings = get_mongo_from_settings


def get_mongo(**kwargs):
    """Returns a redis client instance.

    Parameters
    ----------
    redis_cls : class, optional
        Defaults to ``redis.StrictRedis``.
    url : str, optional
        If given, ``redis_cls.from_url`` is used to instantiate the class.
    **kwargs
        Extra parameters to be passed to the ``redis_cls`` class.

    Returns
    -------
    server
        Redis client instance.

    """
    mongo_cls = kwargs.pop('mongo_cls', defaults.MONGO_CLS)
    database = kwargs.pop('database', defaults.MONGO_DATABASE)
    url = kwargs.pop('url', None)
    if url:
        return mongo_cls(url, **kwargs)[database]
    else:
        return mongo_cls(**kwargs)[database]
