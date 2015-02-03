#!/usr/bin/python

import subprocess


def test(path="", options=None):

    # command
    command = ["-test", path]

    # command options
    option = {"e": "-e",
              "f": "-f",
              "z": "-z",
              "s": "-s",
              "d": "-d"}.get(options, None)

    if option:
        # add options to the command list
        command.insert(1, option)

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def ls(path=None, output=None):

    # command
    command = ["-ls"]

    if path:
        command.append(path)

    std_out, std_err, exit_code = __execute__(command)

    if exit_code == 1:
        # print std_err
        return None
    else:
        results = std_out.split()
        if results[0] == "Found" and results[2] == "items":
            del results[:3]

        if output == 'stdout':
            return std_out
        else:
            return results[7::8]


def count(path, output=None):

    # command
    command = ["-count", path]

    std_out, std_err, exit_code = __execute__(command)

    if output == 'stdout':
        return std_out
    else:
        results = std_out.split()
        return int(results[0]), int(results[1]), int(results[2]), results[3]


def mkdir(path, options=None):

    # assert (type(path) == str) or (type(path) == list), "path must either be a string or list of strings."

    # command
    command = ["-mkdir"]

    if type(path) == str:
        command.append(path)
    else:
        command.extend(path)

    # command options
    option = {"p": "-p"}.get(options, None)

    if option:
        # add options to the command list
        command.insert(1, option)

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def rm(path, options=None):

    assert (type(path) == str) or (type(path) == list), "path must either be a string or list of strings."

    # command
    command = ["-rm"]

    if type(path) == str:
        command.append(path)
    else:
        command.extend(path)

    # command options
    option = {"r": "-r"}.get(options, None)

    if option:
        # add options to the command list
        command.insert(1, option)

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def mv(path_source, path_destination):

    # command
    command = ["-mv", path_source, path_destination]

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def put(path_source, path_destination=None):

    assert (type(path_source) == str) or (type(path_source) == list), "path must either be a string or list of strings."

    # command
    command = ["-put"]

    if type(path_source) == str:
        command.append(path_source)
    else:
        command.extend(path_source)

    if path_destination:
        command.append(path_destination)
    else:
        command.append(".")

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def __execute__(command):

    # hdfs command
    hdfs_command = ["hdfs", "dfs"]
    hdfs_command.extend(command)

    # run command
    hdfs_cmd = subprocess.Popen(hdfs_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = hdfs_cmd.communicate()
    exit_code = hdfs_cmd.returncode

    if exit_code < 0:
        print std_err
        raise SystemExit

    return std_out, std_err, exit_code


if __name__ == '__main__':

    print "\nHDFS Command: test"
    assert test("temp", "e") == 0
    print "[x] passed true directory test"
    assert test("abc", "e") == 1
    print "[x] passed false directory test"

    print "\nHDFS Command: ls"
    assert len(ls("algorithms")) == 2
    print "[x] passed true directory contents test"
    assert type(ls("algorithms", output='stdout')) == str
    print "[x] passed true directory stdout test"

    print "\nHDFS Command: count"
    print count("algorithms")

    print "\nHDFS Command: mkdir"
    assert mkdir("test") == 0
    print "[x] passed true directory creation test"
    assert mkdir("test1/test2", options="p") == 0
    print "[x] passed true directory structure creation test"

    print "\nHDFS Command: mv"
    assert mv("test", "test_moved") == 0
    print "[x] passed true move directory test"

    print "\nHDFS Command: rm"
    assert rm("test_moved", options="r") == 0
    print "[x] passed delete directory test"
    assert rm("test1", options="r") == 0
    print "[x] passed delete directory structure test"

    print "\nHDFS Command: put"
    assert put("testfile") == 0
    assert put(["testfile1", "testfile2"]) == 0
    print ls("testf*")
    rm("test*")
    print ls("testf*")