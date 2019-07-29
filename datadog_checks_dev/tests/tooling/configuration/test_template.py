# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import re

import pytest

from datadog_checks.dev import TempDir
from datadog_checks.dev.tooling.configuration.template import ConfigTemplates
from datadog_checks.dev.utils import ensure_parent_dir_exists, path_join, write_file

pytestmark = [pytest.mark.conf, pytest.mark.conf_template]


class TestLoadBasic:
    def test_default(self):
        templates = ConfigTemplates()

        assert templates.load('tags/init_config') == {
            'name': 'tags',
            'value': {
                'example': ['<KEY_1>:<VALUE_1>', '<KEY_2>:<VALUE_2>'],
                'type': 'array',
                'items': {'type': 'string'},
            },
            'description': (
                'A list of tags to attach to every metric and service check emitted by this integration.\n'
                '\n'
                'Learn more about tagging at https://docs.datadoghq.com/tagging\n'
            ),
        }

    def test_custom_path_precedence(self):
        with TempDir() as d:
            template_file = path_join(d, 'tags', 'init_config.yaml')
            ensure_parent_dir_exists(template_file)
            write_file(template_file, 'test:\n- foo\n- bar')

            templates = ConfigTemplates([d])

            assert templates.load('tags/init_config') == {'test': ['foo', 'bar']}

    def test_cache(self):
        with TempDir() as d:
            template_file = path_join(d, 'tags', 'init_config.yaml')
            ensure_parent_dir_exists(template_file)
            write_file(template_file, 'test:\n- foo\n- bar')

            templates = ConfigTemplates([d])
            templates.load('tags/init_config')
            write_file(template_file, '> invalid')

            assert templates.load('tags/init_config') == {'test': ['foo', 'bar']}

    def test_unknown_template(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template `unknown` does not exist$'):
            templates.load('unknown')

    def test_parse_error(self):
        with TempDir() as d:
            template_file = path_join(d, 'invalid.yaml')
            ensure_parent_dir_exists(template_file)
            write_file(template_file, '> invalid')

            templates = ConfigTemplates([d])

            with pytest.raises(ValueError, match='^Unable to parse template `{}`'.format(re.escape(template_file))):
                templates.load('invalid')


class TestLoadBranches:
    def test_mapping(self):
        templates = ConfigTemplates()

        assert templates.load('tags/init_config.value.example') == ['<KEY_1>:<VALUE_1>', '<KEY_2>:<VALUE_2>']

    def test_mapping_not_found(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template `tags/init_config` has no element `value.foo`$'):
            templates.load('tags/init_config.value.foo')

    def test_list(self):
        templates = ConfigTemplates()

        assert templates.load('http/instances.skip_proxy.value') == {'example': False, 'type': 'boolean'}

    def test_list_not_found(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template `http/instances` has no named element `foo`$'):
            templates.load('http/instances.foo')

    def test_primitive(self):
        templates = ConfigTemplates()

        assert templates.load('http/instances.skip_proxy.value.example') is False

    def test_primitive_recurse(self):
        templates = ConfigTemplates()

        with pytest.raises(
            ValueError,
            match=(
                '^Template `http/instances.skip_proxy.value.example` does '
                'not refer to a mapping, rather it is type `bool`$'
            ),
        ):
            templates.load('http/instances.skip_proxy.value.example.foo')


class TestLoadOverride:
    def test_mapping(self):
        templates = ConfigTemplates()

        assert templates.load('tags/init_config', parameters={'overrides': {'value.example': ['foo', 'bar']}}) == {
            'name': 'tags',
            'value': {'example': ['foo', 'bar'], 'type': 'array', 'items': {'type': 'string'}},
            'description': (
                'A list of tags to attach to every metric and service check emitted by this integration.\n'
                '\n'
                'Learn more about tagging at https://docs.datadoghq.com/tagging\n'
            ),
        }

    def test_mapping_create(self):
        templates = ConfigTemplates()

        assert templates.load('tags/init_config', parameters={'overrides': {'foo': 'foo'}}) == {
            'name': 'tags',
            'value': {
                'example': ['<KEY_1>:<VALUE_1>', '<KEY_2>:<VALUE_2>'],
                'type': 'array',
                'items': {'type': 'string'},
            },
            'description': (
                'A list of tags to attach to every metric and service check emitted by this integration.\n'
                '\n'
                'Learn more about tagging at https://docs.datadoghq.com/tagging\n'
            ),
            'foo': 'foo',
        }

    def test_mapping_create_nested(self):
        templates = ConfigTemplates()

        assert templates.load('tags/init_config', parameters={'overrides': {'foo.bar': 'foobar'}}) == {
            'name': 'tags',
            'value': {
                'example': ['<KEY_1>:<VALUE_1>', '<KEY_2>:<VALUE_2>'],
                'type': 'array',
                'items': {'type': 'string'},
            },
            'description': (
                'A list of tags to attach to every metric and service check emitted by this integration.\n'
                '\n'
                'Learn more about tagging at https://docs.datadoghq.com/tagging\n'
            ),
            'foo': {'bar': 'foobar'},
        }

    def test_mapping_with_branches(self):
        templates = ConfigTemplates()

        assert templates.load('tags/init_config.value', parameters={'overrides': {'example': ['foo', 'bar']}}) == {
            'example': ['foo', 'bar'],
            'type': 'array',
            'items': {'type': 'string'},
        }

    def test_list(self):
        templates = ConfigTemplates()

        template = templates.load('http/instances', parameters={'overrides': {'skip_proxy.description': 'foobar'}})

        assert {
            'name': 'skip_proxy',
            'value': {'example': False, 'type': 'boolean'},
            'description': 'foobar',
        } in template

    def test_list_with_branches(self):
        templates = ConfigTemplates()

        template = templates.load('http/instances.skip_proxy', parameters={'overrides': {'description': 'foobar'}})

        assert template == {
            'name': 'skip_proxy',
            'value': {'example': False, 'type': 'boolean'},
            'description': 'foobar',
        }

    def test_list_replace(self):
        templates = ConfigTemplates()

        original_template = templates.load('http/instances')
        index = next(i for i, item in enumerate(original_template) if item.get('name') == 'skip_proxy')  # no cov

        template = templates.load('http/instances', parameters={'overrides': {'skip_proxy': 'foobar'}})

        assert 'foobar' in template
        assert template.index('foobar') == index
        template.remove('foobar')

        for item in template:
            assert item.get('name') != 'skip_proxy'

    def test_list_not_found(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template override `proxy.value.properties` has no named mapping `foo`$'):
            templates.load('http/instances', parameters={'overrides': {'proxy.value.properties.foo.foo': 'bar'}})

    def test_list_not_found_root(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template override has no named mapping `foo`$'):
            templates.load('http/instances', parameters={'overrides': {'foo': 'bar'}})

    def test_primitive(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template override `proxy.description` does not refer to a mapping$'):
            templates.load('http/instances', parameters={'overrides': {'proxy.description.foo': 'bar'}})

    def test_primitive_recurse(self):
        templates = ConfigTemplates()

        with pytest.raises(ValueError, match='^Template override `proxy.description` does not refer to a mapping$'):
            templates.load('http/instances', parameters={'overrides': {'proxy.description.foo.foo': 'bar'}})
