from isolate.backends.common import replace_dir


def test_replace_dir(tmp_path):
    src_path = tmp_path / "src"
    src_path.mkdir()

    (src_path / "file").write_text("hello")
    (src_path / "subdir").mkdir()
    (src_path / "subdir" / "file").write_text("hello")

    dst_path = tmp_path / "dst"
    replace_dir(src_path, dst_path)
    assert not src_path.exists()
    assert dst_path.exists()
    assert (dst_path / "file").read_text() == "hello"
    assert (dst_path / "subdir" / "file").read_text() == "hello"


def test_replace_dir_with_existing_dst(tmp_path):
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

    replace_dir(src_path, dst_path)
    assert not src_path.exists()
    assert dst_path.exists()
    assert (dst_path / "file").read_text() == "hello"
    assert (dst_path / "subdir" / "file").read_text() == "hello"
