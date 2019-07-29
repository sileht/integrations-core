# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from textwrap import dedent

from datadog_checks.dev.tooling.configuration import ConfigSpec


def get_spec(contents, **kwargs):
    kwargs.setdefault('integration', 'test')
    return ConfigSpec(dedent(contents), **kwargs)
