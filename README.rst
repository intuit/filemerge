=========
Filemerge
=========

Filemerge is a utility for merging a large number of small HDFS files into
smaller number of large files. Filemerge is intended for use by Hadoop operations
engineers and map-reduce application developers.

The structure of the code is simple. The actual merging is performed by a Pig
script created at run time using user-supplied parameters. These parameters
control the set of files to merge. The utility consists of a single file,
``filemerge.py``, that takes the input parameters and invokes the created pig
script. As such, ``pig`` **command needs to be available and in path of the
runtime user.** The user specifies the input path, output path, topic, and
files to be merged either as a year/month/day format or specific HDFS directory
or a list of HDFS directories in a file.

========================
Installation and testing
========================

Because the application code is small and self-contained, installation requires
simply cloning the repository.


.. code-block:: sh

    git clone https://github.com/intuit/filemerge.git


Note that ``filemerge`` itself does not have any dependencies besides pig
command-line. However, running the test suite locally requires installation of
the test discovery and mocking packages. These dependencies are listed in
``filemerge/requirements.txt`` and can be installed as follows.

.. code-block:: sh

    cd filemerge
    pip install -r requirements.txt

Finally, installation can be verified by running the test suite locally.

.. code-block:: sh

    nosetests -w unit_tests -v


==================
Running the script
==================

--------------
The script API
--------------

The full API of the script is available on commandline by typing

.. code-block:: sh

    python filemerge.py -h

The help message is reproduced below for reference.

.. code-block:: sh   

    Usage:
        python filemerge.py --topic=<'topic-in-single-quotes'>
                            --input-prefix=<'HDFS-location-in-single-quotes'>
                            --output-prefix=<'HDFS-location-in-single-quotes'>
                            --num-reducers=<any-positive-integer>
                            --queue=<hadoop queue name>
                            [--year=<4-digit-year>]
                            [--month=<month>]
                            [--day=<day>]
                            [--dir=<directory relative to input-prefix>]
                            [--file=<file with list of directories, relative to input-prefix>]
                            [--window=<window size in days>]
                            [--codec=<valid hadoop compression codec>]
                            [-r]



    Options:
      -h, --help            show this help message and exit
      -y YEAR, --year=YEAR  Year for the merge
      -m MONTH, --month=MONTH
                            Month for the merge
      -d DAY, --day=DAY     Day for the merge
      -D DIRECTORY, --directory=DIRECTORY
                            Directory containing files to merge
      -f FILE, --file=FILE  File containing list of input directories
      -w WINDOW, --window=WINDOW
                            Window in days (merge for the past *n* days
      -l LOOKBACK, --lookback=LOOKBACK
                            Lookback period (merge for 1 day *n* days prior)
      -t TOPIC, --topic=TOPIC
                            Topic for the merge
      -i INPUT_PREFIX, --input-prefix=INPUT_PREFIX
                            Input directory prefix
      -o OUTPUT_PREFIX, --output-prefix=OUTPUT_PREFIX
                            Output directory prefix
      -n NUM_REDUCERS, --num-reducers=NUM_REDUCERS
                            Number of reducers
      -c CODEC, --codec=CODEC
                            Compression codec to use
      -q QUEUE, --queue=QUEUE
                            Mapreduce job queue
      -r, --dry-run         Dry run; create, but dont execute the Pig script

The arguments outside the square brackets are required and those in the square
brackets are optional, but a minimum set of these arguments is needed to compute
the set of directories to be merged. The acceptable option groups are following:

 - Group 1
    - year (-y)
    - year (-y), month (-m)
    - year (-y), month (-m), day (-d)

 - Group 2
    - HDFS directory (-D)

 - Group 3
    - file with a list of HDFS directories (-f)

 - Group 4
    - window with a start date (-w); files for all days between start date minus
      window to start date will be merged

 - Group 5
    - lookback with a start date (-l); files for a single day lookback days before the
      start date will be merged

These option groups are designed to enable merging at the directory, day, month,
or the year level. The ``-f`` offers ability to merge non-contiguous firectory
blocks. The ``-w`` and ``-l`` options allow merging of directories at periodic
intervals using a sliding window.

One can further enhance the flexibility of these options by wrapping the
``python`` call in a shell script and providing custom list of directories,
non-contiguous months, shunking large directory lists into smaller parts etc.

++++++++++++++++++++++++
Why all the flexibility?
++++++++++++++++++++++++

The ``filemerge`` tool is written with operations and map-reduce application
developers in mind. Operations team will need periodic merges based on the
retention policy and will typically use the tool with the ``-y, -m, -d``
options. Map-reduce application developers might need to merge single
directories or random directory groups and will use the ``-d`` and ``-f``
options.

---------------------------------------------
Basic usage: Merging all files in a directory
---------------------------------------------

The most common usage pattern for ``filemerge`` is to merge all files in a
directory and produce one output file (in a different directory). To merge files
unders a specific directory, provide the basepath using the ``-i`` option and
the final directory name using the ``-D`` option. In the following invocation
the ``/path/to/clickstream`` is the base HDFS path and ``jan2016`` is the
subdirectory that contains the files to be merged (in this case, for January
2016). In other words, the full path to the files that will be merged is:
``/path/to/clickstream/jan2016``

.. code-block:: sh

    python filemerge/filemerge.py \
        -i '/hdfs/path/to/clickstream' \
        -D 'jan2016' \
        -o '/hdfs/path/to/jan2016-merged' \
        -t 'clickstream'


-----------------------------------------
Example invocation for a full month merge
-----------------------------------------

Following command invokes the script for merging February 2015 data of the
'clickstream' directory in HDFS. This is the raw call to the filemerge python script
and will initiate 28 map-reduce jobs.

.. code-block:: sh
    
    python filemerge/filemerge.py \
        -i '/hdfs/path/to/clickstream' \
        -o '/hdfs/path/to/clickstream-merged' \
        -t 'clickstream' \
        -y 2015 \
        -m 2

----------------------------------------        
Example invocation for a full year merge
----------------------------------------

Simply omit the month and day options and the merge wil be performed for the
full year. Following command invokes the script for merging the entire 2015 data
of the 'clickstream' directory with a 1 day chunk size. This will initiate 365
map-reduce jobs.

.. code-block:: sh
    
    python filemerge/filemerge.py \
        -i '/hdfs/path/to/clickstream' \
        -o '/hdfs/path/to/clickstream-merged' \
        -t 'clickstream' \
        -y 2015

Note that detecting files in time window (e.g. a certain month or a year)
requires ``filemerge`` to assume certain directory naming conventions. This
convention is specified in ``filemerge/templates.py`` and can be user-defined.

------------------------------------------------------
Example invocation for a non-contiguous directory list
------------------------------------------------------

To merge files under unrelated non-contiguous directories, list all the final
directory names in a file and pass the full file path to the ``-f`` option. In
the invocation below, ``-i`` captures the common portion of the path to all the
directories and the final directories are listed in the file.

.. code-block:: sh

    python filemerge/filemerge.py \
        -i '/hdfs/path/to/clickstream' \
        -o '/hdfs/path/to/clickstream-merged' \
        -t 'clickstream' \
        -f /local/filesystem/path/to/directory_list.txt

Lets assume to that ``/local/filesystem/path/to/directory_list.txt`` contains
the following lines

.. code-block:: sh

    d_20150225
    d_20160309
    d_20150728

In that case all files under ``/hdfs/path/to/clickstream/{d_20150225,
d_20160309, d_20150728}`` will be merged. Note, that they wont be merged into
the *same* file. Rather, three different output directories, one for each directory
in listed in ``directory_list.txt``, will be created.

--------------------------------------------
Example invocation for a sliding time window
--------------------------------------------

The following invocation the ``filemerge`` script will merge files in the
``clickstream`` directory for the last 20 days (not including today). The window
is datetime aware.

.. code-block:: sh

    python filemerge/filemerge.py \
        -i '/hdfs/path/to/clickstream' \
        -o '/hdfs/path/to/clickstream-merged' \
        -t 'clickstream' \
        -w 20

---------------------------------------------------
Example invocation for a sliding window daily merge
---------------------------------------------------

The following invocation the ``filemerge`` script will merge files in the
``clickstream`` topic for the day 20 days prior to today. The lookback is
datetime aware.

.. code-block:: sh

    python filemerge/filemerge.py \
        -i '/hdfs/path/to/clickstream' \
        -o '/hdfs/path/to/clickstream-merged' \
        -t 'clickstream' \
        -l 20

---------------------
Multi-directory merge
---------------------

For multi-directory merges, ``filemerge.py`` can be called from a script that
provides the list of directories and the merge frequency. The following wrapper
script shows how to merge 2015 files for a subset of directories. The script needs to
be present in the same directory as the ``filemerge.py`` script.

 .. code-block:: sh

    #!/bin/bash

    # List of all HDFS subdirectories can be obtained as follows
    # hadoop fs -ls /hdfs/base/path | sed -E "s:.*/hdfs/base/path/(.*)$:\\1:"
     
    # Set of subdirectories to be merged, obtained from output of the
    # above command

    TOPICS=(
        businessevents
        customer-transactions
        desktop-clickstream
        mobile-clickstream-ios
        mobile-clickstream-android)
    
    YEAR=2015
    for TOPIC in ${TOPICS[@]}; do
        OUTPUT_DIR="/hdfs/base/path/${TOPIC}-merged"

        python filemerge/filemerge.py \
            -i '/hdfs/base/path/${TOPIC}' \
            -o ${OUTPUT_DIR} \
            -t ${TOPIC} \
            -y 2015
    done

-----------------------
Merge for custom months
-----------------------

Merging for custom months is straightforward and is similar to above looping
logic. Once again, the following script needs to be located in the same directory
as ``filemerge.py``.

 .. code-block:: sh

    #!/bin/bash

    # Subset of months to be merged
    MONTHS=(
        01 
        02
        07
        09
        10
        12)
    
    YEAR=2015
    TOPIC="clickstream"
    OUTPUT_DIR="/hdfs/base/path/${TOPIC}-merged"

    for MM in ${MONTHS[@]}; do        
        python filemerge/filemerge.py \
            -i '/hdfs/base/path/${TOPIC}' \
            -o ${OUTPUT_DIR} \
            -t ${TOPIC} \
            -y 2015 \
            -m ${MONTH}

    done

------------------
High-level pattern
------------------

The overarching pattern here is to realize that **the unit of time for the merge
logic is a directory**. As long as this is noted, the actual logic can be customized
in more ways than those shown above: simply write a wrapper shell script to
create your variables and loop over them. These variables can be months,
input directories, or output directories.