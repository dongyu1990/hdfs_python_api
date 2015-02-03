#!/usr/bin/python

import subprocess


def ls(path=None, output=None):

    # command
    command = ["-ls"]

    command.extend(__paths_type_check__(path))

    std_out, std_err, exit_code = __execute__(command)

    if exit_code == 1:
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
    command = ["-count"]

    command.extend(__paths_type_check__(path))

    std_out, std_err, exit_code = __execute__(command)

    if output == 'stdout':
        return std_out
    else:
        results = std_out.split()
        return int(results[0]), int(results[1]), int(results[2]), results[3]


def mv(path_source, path_destination):

    # command
    command = ["-mv"]

    command.extend(__paths_type_check__(path_source))
    command.extend(__paths_type_check__(path_destination))

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def mkdir(paths, options=None):

    # command
    command = ["-mkdir"]

    # command options
    command_options = {"p": "-p"}

    if options:
        command.extend(__add_options__(options, command_options))

    command.extend(__paths_type_check__(paths))

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def rm(paths, options=None):

    # command
    command = ["-rm"]

    # command options
    command_options = {"r": "-r",
                       "f": "-f"}

    if options:
        command.extend(__add_options__(options, command_options))

    command.extend(__paths_type_check__(paths))

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def put(paths_source, path_destination=None):

    # command
    command = ["-put"]

    command.extend(__paths_type_check__(paths_source))

    if path_destination:
        command.append(path_destination)
    else:
        command.append(".")

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def test(path="", options=None):

    # command
    command = ["-test"]

    # command options
    command_options = {"e": "-e",
                       "f": "-f",
                       "z": "-z",
                       "s": "-s",
                       "d": "-d"}

    if options:
        command.extend(__add_options__(options, command_options))

    command.extend(__paths_type_check__(path))

    std_out, std_err, exit_code = __execute__(command)

    return exit_code


def __paths_type_check__(paths):

    command = []

    if isinstance(paths, basestring):
        command.append(paths)
    elif all(isinstance(path, basestring) for path in paths):
        command.extend(paths)
    else:
        raise TypeError("path must either be a string or a list of strings", paths)

    return command


def __add_options__(options, command_options):

    cmd_options = []

    for option in options:
        cmd_options.append(command_options.get(option))

    return filter(None, cmd_options)


def __execute__(command):

    # hdfs command
    hdfs_command = ["hdfs", "dfs"]
    hdfs_command.extend(command)

    print hdfs_command

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