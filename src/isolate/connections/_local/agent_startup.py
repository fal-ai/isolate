"""
Agent process execution wrapper for handling extended PYTHONPATH.
"""

import os
import runpy
import site
import sys


def load_pth_files() -> None:
    """Each site dir in Python can contain some .pth files, which are
    basically instructions that tell Python to load other stuff. This is
    generally used for editable installations, and just setting PYTHONPATH
    won't make them expand so we need manually process them. Luckily, site
    module can simply take the list of new paths and recognize them.

    https://docs.python.org/3/tutorial/modules.html#the-module-search-path
    """
    python_path = os.getenv("PYTHONPATH")
    if python_path is None:
        return None

    # TODO: The order here is the same as the one that is used for generating the
    # PYTHONPATH. The only problem that might occur is that, on a chain with
    # 3 ore more nodes (A, B, C), if X is installed as an editable package to
    # B and a normal package to C, then C might actually take precedence. This
    # will need to be fixed once we are dealing with more than 2 nodes and editable
    # packages.
    for site_dir in python_path.split(os.pathsep):
        site.addsitedir(site_dir)


def main():
    real_agent, *real_arguments = sys.argv[1:]

    load_pth_files()
    # TODO(feat): implement a check to parse "agent-requires" line and see if
    # all the dependencies are installed.
    sys.argv = [real_agent] + real_arguments
    runpy.run_path(real_agent, run_name="__main__")


if __name__ == "__main__":
    load_pth_files()
    main()
