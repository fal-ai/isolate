from setuptools import setup

setup(
    name="isolate-ipython",
    version="0.0.1",
    packages=["isolate_ipython"],
    entry_points={
        "console_scripts": [
            "fal-notebook = isolate_ipython.__main__:main",
        ]
    },
    install_requires=["isolate[grpc]==0.3.0", "dill==0.3.5.1"],
)
