# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from itertools import chain

import clickhouse_driver
from six import raise_from

from datadog_checks.base import AgentCheck, is_affirmative
from datadog_checks.base.utils.containers import iter_unique

from . import queries
from .exceptions import QueryExecutionError
from .utils import ErrorSanitizer


class ClickhouseCheck(AgentCheck):
    __NAMESPACE__ = 'clickhouse'
    SERVICE_CHECK_CONNECT = 'can_connect'

    def __init__(self, name, init_config, instances):
        super(ClickhouseCheck, self).__init__(name, init_config, instances)

        self._server = self.instance.get('server', '')
        self._port = self.instance.get('port')
        self._db = self.instance.get('db', 'default')
        self._user = self.instance.get('user', 'default')
        self._password = self.instance.get('password', '')
        self._connect_timeout = float(self.instance.get('connect_timeout', 10))
        self._read_timeout = float(self.instance.get('read_timeout', 10))
        self._ping_timeout = float(self.instance.get('ping_timeout', 5))
        self._compression = self.instance.get('compression', False)
        self._tls_verify = is_affirmative(self.instance.get('tls_verify', False))
        self._tags = self.instance.get('tags', [])

        # Add global tags
        self._tags.append('server:{}'.format(self._server))
        self._tags.append('port:{}'.format(self._port))
        self._tags.append('db:{}'.format(self._db))

        custom_queries = self.instance.get('custom_queries', [])
        use_global_custom_queries = self.instance.get('use_global_custom_queries', True)

        # Handle overrides
        if use_global_custom_queries == 'extend':
            custom_queries.extend(self.init_config.get('global_custom_queries', []))
        elif 'global_custom_queries' in self.init_config and is_affirmative(use_global_custom_queries):
            custom_queries = self.init_config.get('global_custom_queries', [])

        # Deduplicate
        self._custom_queries = list(iter_unique(custom_queries))

        # We'll connect on the first check run
        self._client = None
        self.check_initializations.append(self.create_connection)

        self._error_sanitizer = ErrorSanitizer(self._password)

        self._collection_methods = (self.query_system_metrics, self.query_system_events)

    def check(self, _):
        for collection_method in self._collection_methods:
            try:
                collection_method()
            except QueryExecutionError as e:
                self.log.error('Error querying %s: %s', e.source, e)
                continue
            except Exception as e:
                self.log.error('Unexpected error running `%s`: %s', collection_method.__name__, e)
                continue

    def query_system_metrics(self):
        # https://clickhouse.yandex/docs/en/operations/system_tables/#system_tables-metrics
        result = self.execute_query(queries.SystemMetrics)
        self.collect_version(extra_version_parts={'revision': str(result['Revision'])})

    def query_system_events(self):
        # https://clickhouse.yandex/docs/en/operations/system_tables/#system_tables-events
        self.execute_query(queries.SystemEvents)

    def query_custom(self):
        for custom_query in self._custom_queries:
            query = custom_query.get('query')
            if not query:  # no cov
                self.log.error('Custom query field `query` is required')
                continue

            columns = custom_query.get('columns')
            if not columns:  # no cov
                self.log.error('Custom query field `columns` is required')
                continue

            self.log.debug('Running custom query for ClickHouse')
            rows = self.iter_rows_raw(query)

            # Trigger query execution
            try:
                first_row = next(rows)
            except Exception as e:  # no cov
                self.log.error('Error executing custom query: %s', e)
                continue

            for row in chain((first_row,), rows):
                if not row:  # no cov
                    self.log.debug('Custom query returned an empty result')
                    continue

                if len(columns) != len(row):  # no cov
                    self.log.error('Custom query result expected %s column(s), got %s', len(columns), len(row))
                    continue

                metric_info = []
                query_tags = list(self._tags)
                query_tags.extend(custom_query.get('tags', []))

                for column, value in zip(columns, row):
                    # Columns can be ignored via configuration.
                    if not column:  # no cov
                        continue

                    name = column.get('name')
                    if not name:  # no cov
                        self.log.error('Column field `name` is required')
                        break

                    column_type = column.get('type')
                    if not column_type:  # no cov
                        self.log.error('Column field `type` is required for column `%s`', name)
                        break

                    if column_type == 'tag':
                        query_tags.append('{}:{}'.format(name, value))
                    else:
                        if not hasattr(self, column_type):
                            self.log.error('Invalid submission method `%s` for metric column `%s`', column_type, name)
                            break
                        try:
                            metric_info.append((name, float(value), column_type))
                        except (ValueError, TypeError):  # no cov
                            self.log.error('Non-numeric value `%s` for metric column `%s`', value, name)
                            break

                # Only submit metrics if there were absolutely no errors - all or nothing.
                else:
                    for info in metric_info:
                        metric, value, method = info
                        getattr(self, method)(metric, value, tags=query_tags)

    def collect_version(self, extra_version_parts=None):
        version = self.execute_query_raw('SELECT version()', lambda: 'version()')[0][0]

        # The version comes in like `19.15.2.2` though sometimes there is no patch part
        version_parts = {name: part for name, part in zip(('year', 'major', 'minor', 'patch'), version.split('.'))}
        if extra_version_parts:
            version_parts.update(extra_version_parts)

        self.set_metadata('version', version, scheme='parts', final_scheme='calver', part_map=version_parts)

    def execute_query(self, query):
        rows = self.execute_query_raw(query.query, lambda: ', '.join(sorted(query.views)))

        # Re-use column access map for efficiency
        result = {}

        # Avoid repeated lookups
        query_name = query.__name__
        columns = query.columns
        ignored_columns = query.ignored_columns
        get_method = getattr

        for column, value in rows:
            metric_data = columns.get(column)
            if metric_data is None:
                if column not in ignored_columns:  # no cov
                    self.log.info('Skipping unknown column `%s` encountered while querying `%s`.', column, query_name)

                continue

            for metric, method in metric_data:
                if metric:
                    if callable(method):
                        method, computed_value = method(value)
                        get_method(self, method)(metric, computed_value, tags=self._tags)
                    else:
                        get_method(self, method)(metric, value, tags=self._tags)

            result[column] = value

        # Return all values for possible post-processing
        return result

    def iter_rows_raw(self, query):
        try:
            rows = self._client.execute_iter(query.query)
            for row in rows:
                yield row
        except Exception as e:
            raise QueryExecutionError(self._error_sanitizer.clean(str(e)), 'custom query')

    def execute_query_raw(self, query, source, stream=False):
        try:
            if stream:
                rows = self._client.execute_iter(query)

                # Trigger query execution
                try:
                    first_row = next(rows)
                except StopIteration:  # no cov
                    return iter([])

                return chain((first_row,), rows)
            else:
                return self._client.execute(query)
        except Exception as e:
            raise QueryExecutionError(self._error_sanitizer.clean(str(e)), source())

    def create_connection(self):
        try:
            client = clickhouse_driver.Client(
                host=self._server,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._db,
                connect_timeout=self._connect_timeout,
                send_receive_timeout=self._read_timeout,
                sync_request_timeout=self._ping_timeout,
                compression=self._compression,
                verify=self._tls_verify,
                # Don't pollute the Agent logs
                settings={'calculate_text_stack_trace': False},
                # Make every client unique for server logs
                client_name='datadog-{}'.format(self.check_id),
            )
            client.connection.connect()
        except Exception as e:
            error = 'Unable to connect to ClickHouse: {}'.format(
                self._error_sanitizer.clean(self._error_sanitizer.scrub(str(e)))
            )
            self.service_check(self.SERVICE_CHECK_CONNECT, self.CRITICAL, message=error, tags=self._tags)

            raise_from(type(e)(error), None)
        else:
            self.service_check(self.SERVICE_CHECK_CONNECT, self.OK, tags=self._tags)
            self._client = client
