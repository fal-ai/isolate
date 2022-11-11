from multiprocessing import Process

from isolate.backends.common import replace_dir


def test_replace_dir(tmp_path):
    lock_dir = tmp_path / "lock"
    lock_dir.mkdir()

    src_path = tmp_path / "src"
    src_path.mkdir()

    (src_path / "file").write_text("hello")
    (src_path / "subdir").mkdir()
    (src_path / "subdir" / "file").write_text("hello")

    dst_path = tmp_path / "dst"
    replace_dir(src_path, dst_path, lock_dir)
    assert not src_path.exists()
    assert dst_path.exists()
    assert (dst_path / "file").read_text() == "hello"
    assert (dst_path / "subdir" / "file").read_text() == "hello"


def test_replace_dir_with_existing_dst(tmp_path):
    lock_dir = tmp_path / "lock"
    lock_dir.mkdir()

    src_path = tmp_path / "src"
    src_path.mkdir()

    (src_path / "file").write_text("hello")
    (src_path / "subdir").mkdir()
    (src_path / "subdir" / "file").write_text("hello")

    dst_path = tmp_path / "dst"
    dst_path.mkdir()
    (dst_path / "file").write_text("yyy")

    (dst_path / "file").write_text("hello")
    (dst_path / "subdir").mkdir()
    (dst_path / "subdir" / "file").write_text("hello")

    replace_dir(src_path, dst_path, lock_dir)
    assert not src_path.exists()
    assert dst_path.exists()
    assert (dst_path / "file").read_text() == "hello"
    assert (dst_path / "subdir" / "file").read_text() == "hello"


def replace_on_process(*args, timeout=0.5):
    process = Process(target=replace_dir, args=args)
    process.start()
    process.join(timeout=timeout)
    process.kill()
    process.join()
    assert not process.is_alive()


def test_await_lock(tmp_path):
    from isolate.backends.common import _lock_path

    lock_dir = tmp_path / "lock"
    lock_dir.mkdir()

    src_path = tmp_path / "src"
    src_path.mkdir()

    (src_path / "file").write_text("hello")

    dst_path = tmp_path / "dst"
    dst_path.mkdir()

    # Try locking the dst_path
    with _lock_path(dst_path, lock_dir):
        # And now try replacing it (should fail because the lock is held)
        replace_on_process(src_path, dst_path, lock_dir)
        assert not (dst_path / "file").exists()

    assert src_path.exists()
    assert dst_path.exists()

    # but if we release the lock, it should work
    replace_on_process(src_path, dst_path, lock_dir)
    assert (dst_path / "file").exists(), list(dst_path.iterdir())
