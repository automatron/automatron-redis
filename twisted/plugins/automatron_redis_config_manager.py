import re
from twisted.internet import defer
from twisted.python import log
from zope.interface import implements, classProvides
from automatron.core.config import IConfigManager, IAutomatronConfigManagerFactory
from automatron_redis.txredisapi import ConnectionPool
from automatron_redis import build_redis_config


class RedisConfigManager(object):
    classProvides(IAutomatronConfigManagerFactory)
    implements(IConfigManager)

    name = 'redis'

    def __init__(self, controller):
        self.redis = None
        self._redis_config = build_redis_config(controller.config_file)

    @defer.inlineCallbacks
    def prepare(self):
        self.redis = yield ConnectionPool(**self._redis_config)

    @defer.inlineCallbacks
    def shutdown(self):
        if self.redis:
            yield self.redis.disconnect()

    @defer.inlineCallbacks
    def enumerate_servers(self):
        server_sections = yield self.redis.keys('automatron:server:*')
        servers = []
        for section in server_sections:
            if (yield self.redis.hget(section, 'hostname')):
                servers.append(section.split(':', 2)[2])
        defer.returnValue(servers)

    def _get_paths(self, section, server, channel):
        section = 'automatron:' + section
        paths = [
            ((section, '', ''), 0),
        ]
        if server:
            paths.append(((section, server, ''), 1))
        if channel:
            paths.append(((section, '', channel), 2))
        if server and channel:
            paths.append(((section, server, channel), 3))

        return tuple(
            (':'.join(path).rstrip(':'), relevance)
            for path, relevance in paths
        )

    @defer.inlineCallbacks
    def get_section(self, section, server, channel):
        config = {}
        paths = self._get_paths(section, server, channel)
        for path, _ in paths:
            config.update((yield self.redis.hgetall(path)))
        defer.returnValue(config)

    @defer.inlineCallbacks
    def get_section_with_relevance(self, section, server, channel):
        config = {}
        paths = self._get_paths(section, server, channel)
        for path, relevance in paths:
            config.update({
                key: (value, relevance)
                for key, value in
                (yield self.redis.hgetall(path)).items()
            })
        defer.returnValue(config)

    def get_plugin_section(self, plugin, server, channel):
        return self.get_section('plugin.%s' % plugin.name, server, channel)

    def delete_section(self, section, server, channel):
        path = ':'.join(['automatron:' + section, server or '', channel or '']).rstrip(':')
        return self.redis.delete(path)

    @defer.inlineCallbacks
    def get_value(self, section, server, channel, key):
        paths = self._get_paths(section, server, channel)
        for path, relevance in reversed(paths):
            exists = yield self.redis.hexists(path, key)
            if exists:
                defer.returnValue(((yield self.redis.hget(path, key)), relevance))
        defer.returnValue((None, None))

    def get_plugin_value(self, plugin, server, channel, key):
        return self.get_value('plugin.%s' % plugin.name, server, channel, key)

    @defer.inlineCallbacks
    def update_value(self, section, server, channel, key, new_value):
        _, relevance = yield self.get_value(section, server, channel, key)
        if relevance is not None:
            if relevance == 0:
                server = channel = ''
            elif relevance == 1:
                channel = ''
            elif relevance == 2:
                server = ''

        config_key = ('automatron:%s:%s:%s' % (section, server or '', channel or '')).rstrip(':')
        yield self.redis.hset(config_key, key, new_value)

    def update_plugin_value(self, plugin, server, channel, key, new_value):
        return self.update_value('plugin.%s' % plugin.name, server, channel, key, new_value)

    def delete_value(self, section, server, channel, key):
        path = ':'.join(['automatron:' + section, server or '', channel or '']).rstrip(':')
        return self.redis.hdel(path, key)

    @defer.inlineCallbacks
    def get_username_by_hostmask(self, server, user):
        masks = yield self.get_section_with_relevance('user.hostmask', server, None)
        for user_regex, (username, relevance) in masks.items():
            if re.match(user_regex, user):
                defer.returnValue((username, relevance))
        else:
            defer.returnValue((None, None))

    @defer.inlineCallbacks
    def get_role_by_username(self, server, channel, username):
        defer.returnValue((yield self.get_value('user.role', server, channel, username)))

    @defer.inlineCallbacks
    def get_permissions_by_role(self, role):
        permissions, _ = yield self.get_value('role.permissions', None, None, role)
        if permissions is None:
            defer.returnValue(None)

        permissions = [p.strip() for p in permissions.split(',')]
        defer.returnValue(permissions)

    @defer.inlineCallbacks
    def has_permission(self, server, channel, user, permission):
        username, username_rel = yield self.get_username_by_hostmask(server, user)
        if username is None:
            defer.returnValue(False)

        role, role_rel = yield self.get_role_by_username(server, channel, username)
        if role is None:
            defer.returnValue(False)

        if role_rel < username_rel:
            defer.returnValue(False)

        permissions = yield self.get_permissions_by_role(role)
        if permissions is None:
            defer.returnValue(False)

        defer.returnValue(bool({'*', permission} & set(permissions)))

    @defer.inlineCallbacks
    def get_user_preference(self, server, username, preference):
        value, _ = yield self.get_value('user.pref', server, username, preference)
        defer.returnValue(value)

    @defer.inlineCallbacks
    def update_user_preference(self, server, username, preference, value):
        _, email_relevance = yield self.get_value('user.email', server, None, username)
        if email_relevance is None:
            log.msg('Something went terribly wrong, username %s was not found' % username)
            return

        if email_relevance == 0:
            server = None

        yield self.update_value('user.pref', server, username, preference, value)
