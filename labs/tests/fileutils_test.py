from labs.commons.fileutils import join_path


def test_join_path():
    path_a = "/var/lib/"
    path_b = "my_path"
    assert f"{path_a}{path_b}" == join_path(path_a, path_b)


def test_join_path_strip():
    path_a = "/var/lib/"
    path_b = "/my_path"
    assert f"{path_a.rstrip('/')}{path_b}" == join_path(path_a, path_b)
