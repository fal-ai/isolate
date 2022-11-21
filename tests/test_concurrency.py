import os
from concurrent import futures
from functools import partial

import isolate

MAX_ENV_CREATE_TIME = 120


def test_concurrent_creation_only():
    environment_1 = isolate.prepare_environment(
        "virtualenv", requirements=["pyjokes==0.6.0"]
    )
    environment_2 = isolate.prepare_environment("conda", packages=["pyjokes=0.5.0"])

    # Ensure that we can create these environments in sync
    # first.
    sync_create_1 = environment_1.create()
    sync_create_2 = environment_2.create()

    # Cleanup the existing environments.
    environment_1.destroy(sync_create_1)
    environment_2.destroy(sync_create_2)

    with futures.ProcessPoolExecutor(os.cpu_count() or 8) as executor:
        # Do some create and destroy operations in parallel and ensure
        # everything works as expected.
        fs = [
            executor.submit(environment.create)
            for environment in [environment_1, environment_2]
            for _ in range(24)
        ]

        done_fs, not_done_fs = futures.wait(fs, timeout=MAX_ENV_CREATE_TIME)
        assert not not_done_fs

        for future in done_fs:
            assert future.exception() is None


def test_concurrency_on_delete():
    environment = isolate.prepare_environment(
        "virtualenv", requirements=["pyjokes==0.6.0"]
    )

    key = environment.create()
    with futures.ProcessPoolExecutor(max_workers=os.cpu_count() or 8) as executor:
        # Do some create and destroy operations in parallel and ensure
        # everything works as expected.

        destroy = partial(environment.destroy, key)
        done_fs, not_done_fs = futures.wait(
            [
                executor.submit(
                    destroy,
                )
                for _ in range(24)
            ],
            timeout=MAX_ENV_CREATE_TIME,
        )
        assert not not_done_fs

        for future in done_fs:
            assert future.exception() is None
