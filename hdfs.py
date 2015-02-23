
import glob
import subprocess


def ls(path_hdfs_source=".", output=None):

    # command
    command = ["-ls"]

    command.extend(__paths_type_check(path_hdfs_source))

    std_out, std_err, exit_code = __execute(command)

    if exit_code == 1:
        print std_err
        return exit_code
    else:
        if output == "stdout":
            return std_out
        else:
            results = std_out.split()
            if results[0] == "Found" and results[2] == "items":
                del results[:3]
            return results[7::8]


def rm(path_hdfs_destination, options=None):

    # command
    command = ["-rm"]

    # command options
    command_options = {"r": "-r",
                       "f": "-f"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(path_hdfs_destination))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def put(path_local_source, path_hdfs_destination=None, options=None):

    if len(path_local_source) == 0:
        print "no files selected to put on to HDFS:", path_local_source
        return 1

    # command
    command = ["-put"]

    # command options
    command_options = {"f": "-f"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(__expand(path_local_source)))

    if path_hdfs_destination:
        command.append(path_hdfs_destination)
    else:
        command.append(".")

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def du(paths_hdfs, options=None, output=None):

    # command
    command = ["-du"]

    # command options
    command_options = {"s": "-s",
                       "h": "-h"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(paths_hdfs))

    std_out, std_err, exit_code = __execute(command)

    if exit_code == 1:
        print std_err
        return exit_code
    else:
        if output == "stdout":
            return std_out
        else:
            results = std_out.split("\n")[:-1]
            return results


def test(path_hdfs, options=None):

    # command
    command = ["-test"]

    # command options
    command_options = {"e": "-e",
                       "f": "-f",
                       "z": "-z",
                       "s": "-s",
                       "d": "-d"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(path_hdfs))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def count(path_hdfs, output=None):

    # command
    command = ["-count"]

    command.extend(__paths_type_check(path_hdfs))

    std_out, std_err, exit_code = __execute(command)

    if output == 'stdout':
        return std_out
    else:
        results = filter(None, [line.split() for line in std_out.split("\n")])
        results = [(int(result[0]), int(result[1]), int(result[2]), result[3]) for result in results]

        if len(results) > 1:
            return results
        else:
            return results[0]


def mv(path_hdfs_source, path_hdfs_destination):

    # command
    command = ["-mv"]

    command.extend(__paths_type_check(path_hdfs_source))
    command.extend(__paths_type_check(path_hdfs_destination))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def mkdir(path_hdfs_destination, options=None):

    # command
    command = ["-mkdir"]

    # command options
    command_options = {"p": "-p"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(path_hdfs_destination))

    std_out, std_err, exit_code = __execute(command)

    return exit_code





def tail(path_hdfs, options=None):

    # command
    command = ["-tail"]

    # command options
    command_options = {"f": "-f"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(path_hdfs))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def touchz(*path_hdfs):

    # command
    command = ["-touchz"]

    command.extend(__paths_type_check(path_hdfs))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def stat(*path_hdfs):

    # command
    command = ["-stat"]

    command.extend(__paths_type_check(path_hdfs))

    std_out, std_err, exit_code = __execute(command)

    return std_out


def get(path_hdfs_source, path_local_destination=".", crc=False, crc_check=False):

    # command
    command = ["-get"]

    if crc:
        command.append("-crc")

    if crc_check:
        command.append("-ignorecrc")

    command.extend(__paths_type_check(path_hdfs_source))

    command.append(path_local_destination)

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def getmerge(path_hdfs_source, path_local_destination="."):

    # command
    command = ["-getmerge", __path_type_check(path_hdfs_source), __path_type_check(path_local_destination)]

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def cp(paths_hdfs_source, path_hdfs_destination, options=None):

    # command
    command = ["-cp"]

    # command options
    command_options = {"f": "-f"}

    if options:
        command.extend(__add_options(options, command_options))

    command.extend(__paths_type_check(paths_hdfs_source))

    command.append(__path_type_check(path_hdfs_destination))

    std_out, std_err, exit_code = __execute(command)

    return exit_code





def cat(paths_hdfs):

    # command
    command = ["-cat"]

    command.extend(__paths_type_check(paths_hdfs))

    std_out, std_err, exit_code = __execute(command)

    return std_out


def moveFromLocal(path_local_source, path_hdfs_destination="."):

    # command
    command = ["-moveFromLocal", __path_type_check(path_local_source), __path_type_check(path_hdfs_destination)]

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def copyFromLocal(path_local_source, path_hdfs_destination=".", options=None):

    # command
    command = ["-copyFromLocal"]

    # command options
    command_options = {"f": "-f"}

    if options:
        command.extend(__add_options(options, command_options))

    command.append(__path_type_check(path_local_source))

    command.append(__path_type_check(path_hdfs_destination))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def copyToLocal(path_hdfs_source, path_local_destination=".", crc=False, crc_check=False):

    # command
    command = ["-copyToLocal"]

    if crc:
        command.append("-crc")

    if crc_check:
        command.append("-ignorecrc")

    command.append(__path_type_check(path_hdfs_source))

    command.append(__path_type_check(path_local_destination))

    std_out, std_err, exit_code = __execute(command)

    return exit_code


def __path_type_check(path):

    if isinstance(path, basestring):
        return path
    else:
        raise TypeError("path must be a string", path)


def __paths_type_check(paths):

    command = []

    if isinstance(paths, basestring):
        command.append(paths)
    elif all(isinstance(path, basestring) for path in paths):
        command.extend(paths)
    else:
        raise TypeError("path must either be a string or a list of strings", paths)

    return command


def __expand(paths):

    expanded_paths = []

    if isinstance(paths, basestring):
        expanded_paths.extend(glob.glob(paths))
    else:
        for path in paths:
            expanded_paths.extend(glob.glob(path))

    return expanded_paths


def __add_options(options, command_options):

    cmd_options = []

    for option in options:
        cmd_options.append(command_options.get(option))

    return filter(None, cmd_options)


def __execute(command):

    # hdfs command
    hdfs_command = ["hdfs", "dfs"]
    hdfs_command.extend(command)

    print "command:", " ".join(hdfs_command)

    # run command
    hdfs_cmd = subprocess.Popen(hdfs_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = hdfs_cmd.communicate()
    exit_code = hdfs_cmd.returncode

    if exit_code == 255:
        print std_err
        raise SystemExit

    return std_out, std_err, exit_code


if __name__ == '__main__':
    pass