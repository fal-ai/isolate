# Isolate

> :warning: **Isolate** is still very young, and none of the APIs should be considered stable.

Run any Python function, with any dependencies, in any machine you want. Isolate offers a
pluggable end-to-end solution for building, managing, and using isolated environments (virtualenv,
conda, remote, and more).

## Try it!

```py
from isolate import Template, LocalBox

# Build you first environment by specifying its kind (like virtualenv or conda)
template = Template("virtualenv")

# Add some packages to it.
template << "pyjokes==0.5.0"

# Forward it to a box (your local machine, or a remote machine)
environment = template >> LocalBox()

# And then, finally try executing some code

def get_pyjokes_version():
    import pyjokes

    return pyjokes.__version__

# If pyjokes==0.6.0 is installed in your local environment, it is going to print
# 0.6.0 here.
print("Installed pyjokes version: ", get_pyjokes_version())

# But if you run the same function in an isolated environment, you'll get
# 0.5.0.
print("Isolated pyjokes version: ", environment.run(get_pyjokes_version))
```

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
