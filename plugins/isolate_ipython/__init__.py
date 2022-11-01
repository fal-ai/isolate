import textwrap

from isolate_ipython.define import Environment, Machine
from isolate_ipython.notebooks import IsolateRunner


def load_ipython_extension(ipython):
    ipython.register_magics(IsolateRunner)
    ipython.push(
        {
            "Environment": Environment,
            "Machine": Machine,
        }
    )
