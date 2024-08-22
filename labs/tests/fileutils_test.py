from labs.commons.fileutils import join_path, read_line


def test_join_path():
    path_a = "/var/lib/"
    path_b = "my_path"
    assert f"{path_a}{path_b}" == join_path(path_a, path_b)


def test_join_path_strip():
    path_a = "/var/lib/"
    path_b = "/my_path"
    assert f"{path_a.rstrip('/')}{path_b}" == join_path(path_a, path_b)


def test_read_line():
    file_path = "/tmp/readline_test.txt"
    lines = ["Hey\n", "teacher\n", "leave\n", "then\n", "kids\n", "alone\n"]
    with open(file_path, mode="w") as fh:
        fh.writelines(lines)

    lines_gen = read_line(file_path)
    for expected_line in lines:
        assert expected_line.rstrip("\n") == next(lines_gen)
