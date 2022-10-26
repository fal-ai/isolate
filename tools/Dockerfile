FROM python:3.9

RUN apt-get update && apt-get install -y git
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

COPY . .
RUN pip install .

ENV ISOLATE_INHERIT_FROM_LOCAL=1

CMD ["python", "-m", "isolate.server.server"]
