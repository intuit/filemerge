# Copyright 2016 Intuit
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import logging
import datetime
import subprocess as sp
from calendar import monthrange
# We use optparse (vs argparse) for Python2.6 compatibility
from optparse import OptionParser
from templates import PIG_TEMPLATE, DATE_TEMPLATE


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] [func=%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class MissingRequiredOptionsException(RuntimeError):
    pass


class InvalidSourceException(RuntimeError):
    pass


def add_options(_parser):
    """
    Adds options to the passed in '_parser'

    :type _parser: OptionParser instance
    :param _parser: passed in parser

    :rtype: None
    :return: None
    """

    _parser.add_option("-y", "--year",
                       dest="year", action="store",
                       help="Year for the merge")

    _parser.add_option("-m", "--month",
                       dest="month", action="store",
                       help="Month for the merge")

    _parser.add_option("-d", "--day",
                       dest="day", action="store",
                       help="Day for the merge")

    _parser.add_option("-D", "--directory",
                       dest="directory", action="store",
                       help="Directory containing files to merge")

    _parser.add_option("-f", "--file",
                       dest="file", action="store",
                       help="File containing list of input directories")

    _parser.add_option("-w", "--window",
                       dest="window", action="store",
                       help="Window in days (merge for the past *n* days")

    _parser.add_option("-l", "--lookback",
                       dest="lookback", action="store",
                       help="Lookback period (merge for 1 day *n* days prior)")

    _parser.add_option("-t", "--topic",
                       dest="topic", action="store",
                       help="Topic for the merge")

    _parser.add_option("-i", "--input-prefix",
                       dest="input_prefix", action="store",
                       help="Input directory prefix")

    _parser.add_option("-o", "--output-prefix",
                       dest="output_prefix", action="store",
                       help="Output directory prefix")

    _parser.add_option("-n", "--num-reducers",
                       dest="num_reducers", action="store",
                       help="Number of reducers")

    _parser.add_option("-c", "--codec",
                       dest="codec", action="store",
                       help="Compression codec to use")

    _parser.add_option("-q", "--queue",
                       dest="queue", action="store",
                       help="Mapreduce job queue")

    _parser.add_option("-r", "--dry-run",
                       dest="dry_run", action="store_true", default=False,
                       help="Dry run; create, but dont execute the Pig script")


def get_compression_codec(codec_type):
    """
    Returns the compression class from Hadoop IO package

    :type codec_type: str
    :param codec_type: Short codec string ('gzip', 'bzip', 'lzo', 'snappy')

    :rtype: str
    :return: Class for the codec
    """

    CODECS = {
        "gzip": "org.apache.hadoop.io.compress.GzipCodec",
        "bzip": "org.apache.hadoop.io.compress.BZip2Codec",
        "lzo": "com.hadoop.compression.lzo.LzopCodec",
        "snappy": "org.apache.hadoop.io.compress.SnappyCodec"
    }

    try:
        return CODECS[codec_type.lower()]
    except KeyError:
        logger.error("Unsupported compression codec")
        raise


def check_options(_parser, _options):
    """
    Checks options and raises OptionParser.error if required options are absent

    :type _parser: OptionParser
    :param _parser: OptionParser instance managing options for Thrive workflow

    :type _options: object
    :param _options: Options object parsed by "_parser"

    :rtype: str
    :return: Type of input source (ymd, or dir_, or file)

    :exception: OptionParser.error
    """

    opterr = False

    # Check required options
    reqd_opts = ["topic", "input_prefix", "output_prefix", "queue"]
    for attr in reqd_opts:
        if not getattr(_options, attr):
            _parser.print_help()
            raise MissingRequiredOptionsException(
                "Required option '%s' missing" % attr)

    # Create mapping of all sources for which values have been supplied
    all_sources = ["year", "file", "directory", "window", "lookback"]
    sources = dict()
    for src in all_sources:
        opt_val = getattr(_options, src)
        if opt_val:
            sources[src] = opt_val

    # Check for conflicting options
    if len(sources.keys()) != 1:
        _parser.print_help()
        raise InvalidSourceException(
            "Exactly one of these options required: [-y | -D | -f | -w | -l]")

    # At this time, we've ensured that sources contains only one key. Return its
    # value
    return sources.keys()[0]


def getpaths_fromymd(input_prefix_, year_, month_=None, day_=None):
    """
    Generates inputs paths from year, month, day

    :type input_prefix_: str
    :param input_prefix_: root folder of the source data

    :type year_: int
    :param year_: Year of the desired CAMUS path

    :type month_: int
    :param month_: Month of the desired CAMUS path

    :type day_: int
    :param day_: Day of the desired CAMUS path

    :rtype: list
    :return: List of tuples containing input path and base directory name
    """

    # Assign start and end month
    if not month_:
        start_month, end_month = 1, 12
    else:
        start_month = end_month = month_

    months = range(start_month, 1 + end_month)

    input_paths = []

    # Loop over months
    for mm in months:
        # Assign start and end days
        if not day_:
            start_day, end_day = 1, monthrange(year_, mm)[1]
        else:
            start_day = end_day = day_

        days = range(start_day, 1 + end_day)

        # Loop over days
        for dd in days:
            dirname = DATE_TEMPLATE % (year_, mm, dd)
            path = os.path.join(input_prefix_, "%s*" % dirname)
            input_paths.append(tuple(["%s-0000" % dirname, path]))

    return input_paths


def getpaths_fromdir(input_prefix_, directory_):
    """
    Generates inputs paths from directory supplied on the command line

    :type input_prefix_: str
    :param input_prefix_: root folder of the source data

    :type directory_: str
    :param directory_: Base folder of the source data

    :rtype: list
    :return: List of tuples containing input path and base directory name
    """
    path = os.path.join(input_prefix_, "%s*" % directory_, "*")
    return [tuple([directory_, path])]


def getpaths_fromfile(input_prefix_, file_handle_):
    """
    Generates inputs paths from file containing directory names

    :type input_prefix_: str
    :param input_prefix_: root folder of the source data

    :type file_handle_: FileIO
    :param input_prefix_: path to the file containing list of directories

    :rtype: list
    :return: List of tuples containing input path and base directory name
    """

    input_paths = []

    for line in file_handle_:
        line = line.strip()
        if line != "":
            dirname = line
            path = os.path.join(input_prefix_, "%s*" % dirname)
            input_paths.append(tuple([dirname, path]))

    return input_paths


def getpaths_fromwindow(input_prefix_, window_, start_date_):
    """
    Generates input paths from a lookback window.

    :type input_prefix_: str
    :param input_prefix_: root folder of the source data

    :type window_: int
    :param input_prefix_: Merge window size in days

    :type start_date_: datetime.datetime.date
    :param input_prefix_: Start date for the merge window

    :rtype: list
    :return: List of tuples containing input path and base directory name
    """
    input_paths = []
    d = start_date_
    for _ in range(window_):
        d -= datetime.timedelta(days=1)
        dirname = DATE_TEMPLATE % (d.year, d.month, d.day)
        path = os.path.join(input_prefix_, "%s*" % dirname)
        input_paths.append(tuple(["%s-0000" % dirname, path]))

    return input_paths


def getpaths_fromlookback(input_prefix_, lookback_, start_date_=None):
    """
    Generates input paths for ONE day *lookback_* days prior to *start_date_*

    :type input_prefix_: str
    :param input_prefix_: root folder of the source data

    :type lookback_: int
    :param lookback_: number of days to count back to get the day to merge

    :type start_date_: datetime.date
    :param start_date_: Start date for lookback

    :rtype: list
    :return: List of tuples containing input path and base directory name
    """
    if not start_date_:
        start_date_ = datetime.datetime.now().date()

    merge_day = start_date_ - datetime.timedelta(days=lookback_)
    return getpaths_fromymd(input_prefix_, merge_day.year,
                            merge_day.month, merge_day.day)


def getpaths(options, mode):
    """
    Generates Pig input paths given the options and mode

    :type options: OptionParser.option
    :param options: Object containing parsed commandline output

    :type mode: str
    :param mode: The input mode (one of "year", "directory", "file",
                 "window" or "lookback")

    :rtype: list
    :return: List of tuples containing input path and base directory name

    """

    input_prefix = options.input_prefix
    # Assign year
    if mode == "year":
        ymd_vals = [getattr(options, attr)
               for attr in ["year", "month", "day"]]
        year, month, day = map(lambda x: int(x) if x else None, ymd_vals)

        return getpaths_fromymd(input_prefix, year, month, day)

    elif mode == "directory":
        return getpaths_fromdir(input_prefix, options.directory)

    elif mode == "file":
        with open(options.file) as fh:
            return getpaths_fromfile(input_prefix, fh)

    elif mode == "window":
        return getpaths_fromwindow(input_prefix,
                                   int(options.window),
                                   datetime.datetime.now().date())

    elif mode == "lookback":
        return getpaths_fromlookback(input_prefix, int(options.lookback))

    else:
        raise RuntimeError("Incorrect input path generation mode")


def materialize(template, substitutions):
    """
    Creates pig script by performing substitutions in the template.

    :type template: str
    :param template: Pig script template

    :type substitutions: dict
    :param substitutions: Dictionary of substitutions

    :rtype: str
    :return: Materialized pig script
    """

    script_str = template
    for param, value in substitutions.items():
        script_str = re.sub(param, str(value), script_str)

    return script_str


def runpig(script_path):
    """
    Runs the generated Pig script

    :type script_path: str
    :param script_path: path to pig script

    :rtype: str
    :return: result of the shell call
    """
    cmd = "pig -f %s" % script_path
    result = sp.check_call(cmd.split(" "))
    return result


def main():
    """
    Main method

    :rtype: None
    :return: None
    """

    USAGE_MSG = \
    """
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

    """

    # Process options
    parser = OptionParser(USAGE_MSG)
    add_options(parser)
    options, args = parser.parse_args()
    mode = check_options(parser, options)

    input_paths = getpaths(options, mode=mode)

    # Assign number of reducers
    num_reducers = options.num_reducers if options.num_reducers else 10

    # Assign compression codec
    if options.codec:
        set_compression_enabled = "set output.compression.enabled true"
        set_compression_codec = "set output.compression.codec %s" % \
                                get_compression_codec(options.codec)
    else:
        set_compression_enabled = "set output.compression.enabled false"
        set_compression_codec = ""

    for dirname, ipath in input_paths:
        substitutions = {
            "@OUTPUT_PATH": os.path.join(options.output_prefix, dirname),
            "@INPUT_PATH": ipath,
            "@NUM_REDUCERS": num_reducers,
            "@SET_COMPRESSION_ENABLED": set_compression_enabled,
            "@SET_COMPRESSION_CODEC": set_compression_codec,
            "@QUEUE": options.queue
        }

        # Generate the Pig script using substitutions
        if not os.path.exists("scripts"):
            os.mkdir("scripts", 0o700)
        pig_script_str = materialize(PIG_TEMPLATE, substitutions)
        filename = os.path.join("scripts", "%s-%s.pig" %(options.topic, dirname))

        # Write the script to a file
        with open(filename, "w") as pigfile:
            pigfile.write(pig_script_str)

        # Run the Pig file
        if not options.dry_run:
            try:
                runpig(filename)
            except sp.CalledProcessError as ex:
                logger.error(ex.message)
                raise
            except Exception as ex:
                logger.info("Error: %s", ex.message)
                raise

if __name__ == "__main__":
    main()