import sys
from collections import defaultdict
from typing import Any, Dict, List

import importlib_metadata


def _distribution_packages() -> Dict[str, List[str]]:
    """Return a mapping of import names to the packages they
    originate from."""
    distributions = defaultdict(list)
    for pkg, pkg_dists in importlib_metadata.packages_distributions().items():
        for distribution in pkg_dists:
            distributions[distribution].append(pkg)

    return distributions


def get_active_environment() -> Dict[str, Any]:
    distributions = _distribution_packages()

    requirements = []
    for module in sys.modules:
        if module in distributions:
            for distribution in distributions[module]:
                try:
                    version = importlib_metadata.version(distribution)
                except importlib_metadata.PackageNotFoundError:
                    print("Couldn't infer the package for: ", distribution)
                    continue
                else:
                    requirements.append(f"{distribution}=={version}")

    return {
        "kind": "virtualenv",
        "configuration": {"requirements": requirements},
    }
