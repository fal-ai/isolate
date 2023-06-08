# For building this image, you need to be in the project
# root and then pass -f tools/Dockerfile to docker build.
#
# $ docker build -f tools/Dockerfile -t isolate_server [--network=host]
#
# This is important since we want to be able to access src/ directory
# for copying the source code into the container (so the dockerfile
# will stay in a different directory than its context).
FROM python:3.9

RUN apt-get update && apt-get install -y git

RUN mkdir -p /opt
RUN git clone https://github.com/pyenv/pyenv --branch v2.3.6 --depth=1 /opt/pyenv
# TODO: Investigate whether we can leverage the compiled pyenv extension.
ENV ISOLATE_PYENV_EXECUTABLE=/opt/pyenv/bin/pyenv

#### CONDA ####
ENV ISOLATE_CONDA_HOME=/opt/conda/bin
ENV CONDA_DIR /opt/conda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda
#### END CONDA ####

#### MAMBA ####
ENV ISOLATE_MAMBA_HOME=/opt/mamba/bin
ENV CONDA_DIR /opt/mamba
RUN mkdir -p /opt/mamba/bin
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj -C /opt/mamba/bin/ --strip-components=1 bin/micromamba
RUN /opt/mamba/bin/micromamba config append channels conda-forge
RUN /opt/mamba/bin/micromamba config append channels pytorch
#### END MAMBA ####

RUN pip install --upgrade pip virtualenv wheel poetry-core

# Since system-level debian does not comply with
# the sysconfig, and we need to install a bunch
# of dependencies (like dbt-core), we are going to
# use a virtualenv.
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY tools/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY . /isolate
RUN pip install /isolate[server,build]

ENV ISOLATE_INHERIT_FROM_LOCAL=1
ENV AGENT_REQUIREMENTS_TXT=/isolate/tools/agent_requirements.txt

CMD ["python", "-m", "isolate.server.server"]
