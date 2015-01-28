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

    if std_err:
        # exit if there is an error
        print std_err
        raise SystemExit

    return exit_code


def __execute__(command):

    # hdfs command
    hdfs_command = ["hdfs", "dfs"]
    hdfs_command.extend(command)

    # run command
    hdfs_cmd = subprocess.Popen(hdfs_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = hdfs_cmd.communicate()
    exit_code = hdfs_cmd.returncode
    return std_out, std_err, exit_code


if __name__ == '__main__':

    print "HDFS Command: test"
    assert test("temp", "e") == 0
    print "[x] passed true directory test"
    assert test("abc", "e") == 1
    print "[x] passed false directory test"