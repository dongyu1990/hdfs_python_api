HDFS Python API
===============

A python interface to the shell commands that directly interact with the Hadoop Distributed File System (HDFS).

Implementation
==============

To use the API, just import the `hdfs.py` file as per usual:

```Python
import hdfs
```

Commands
========

A full list of the hdfs shell commands and their usages can be found at the main site for [Apache Hadoop](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html) . A list of the commands that have been added to the python api are shown below.

**ls**

hdfs.ls(path, output=None)

path is either a string or list of strings. By default the output is a list of strings, to retrieve the raw output change the output option to *"stdout"*.

```Python   
hdfs.ls("temp")
hdfs.ls("temp", output="stdout")
```