# Isolate

Run any Python function, with any dependencies, in any machine you want. Isolate offers a
pluggable end-to-end solution for building, managing, and using isolated environments (virtualenv,
conda, remote, and more).


## Motivation

![XKCD 1987](https://imgs.xkcd.com/comics/python_environment.png)

The fact that nearly every piece of software uses some other libraries or some
other programs is undeniable. Each of these come with their set of dependencies,
and this chain moves forward. Once there are enough 'nodes' in the chain, then
the ["dependency mess"](https://en.wikipedia.org/wiki/Dependency_hell) starts
to surface and our lives become much harder.

Python tried to solve it by recommending the "virtual environment" concept. In
theory it was designed to isolate environments of different projects, so my project
A can depend on `pandas==1.0.0` while B depends on `pandas==2.0.0` and whichever
project I choose to work with, I just activate its own environment.

Overall this was a very nice solution that did work, and still continues to work
for this use case. But as with every other scoped fix, in time other problems started
to appear that demand a much narrower scope (like defining module-level dependencies,
or even function-level ones for cloud runtimes that allow seamless integration with the
rest of your code running in a different machine).

However, unlike "virtual environment" concept, each of the projects that tried to tackle
this problem lacked a universal interface which one can simply define a set of requirements
(this might be dependencies, size of the machine that is needed to run it, or something completely
different) and can change it without any loss. Isolate is working towards a future where this
transititon is as seamless as the transition from your local environment to the remote
environment.

## Contributing

### Installing in editable mode with dev dependencies

```
pip install -e '.[dev]'
```

### Running tests

```
pytest
```

### Pre-commit

```
pre-commit install
```

### Commit format

Please follow [conventional commits specification](https://www.conventionalcommits.org/) for descriptions/messages.

