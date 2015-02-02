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

    std_out, exit_code = __execute__(command)

    return exit_code


def ls(path=None, output=None):

    # command
    command = ["-ls"]

    if path:
        command.append(path)

    std_out, exit_code = __execute__(command)

    if output == 'stdout':
        return std_out
    else:
        return std_out.split()[2::8][1:]


def count(path, output=None):

    # command
    command = ["-count", path]

    std_out, exit_code = __execute__(command)

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

    std_out, exit_code = __execute__(command)

    return exit_code


def rm(path, options=None):

    # assert (type(path) == str) or (type(path) == list), "path must either be a string or list of strings."

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

    std_out, exit_code = __execute__(command)

    return exit_code


def mv(path_source, path_destination):

    # command
    command = ["-mv", path_source, path_destination]

    std_out, exit_code = __execute__(command)

    return exit_code


def __execute__(command):

    # hdfs command
    hdfs_command = ["hdfs", "dfs"]
    hdfs_command.extend(command)

    # run command
    hdfs_cmd = subprocess.Popen(hdfs_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = hdfs_cmd.communicate()
    exit_code = hdfs_cmd.returncode

    if std_err and exit_code < 0:
        # exit if there is an error
        print std_err
        raise SystemExit

    return std_out, exit_code


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
    print mkdir("test")
    print mkdir("test1/test2", options="p")

    print "\nHDFS Command: mv"
    print mv("test", "test_moved")

    print "\nHDFS Command: rm"
    print rm("test_moved", options="r")
    print rm("test1", options="r")