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
import unittest
import datetime
import mock
from calendar import monthrange
from optparse import OptionParser
import re
import tempfile
import filemerge.filemerge as fm
import subprocess as sp

class Bunch(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def squeeze(str_):
    return re.sub("\s+", " ", str_).strip()


def make_tempfile(prefix="__test__", suffix=".tmp", dir_="."):
    tf = tempfile.NamedTemporaryFile(suffix=suffix, prefix=prefix, dir=dir_)
    return tf


def tempfile_write(fileobj, txt):
    fileobj.write(txt)
    fileobj.seek(0)


OPTIONS_KEYS = ["month", "topic", "directory",
                "file", "year", "day",
                "dry_run", "input_prefix", "lookback",
                "queue", "window", "output_prefix",
                "num_reducers", "codec"]


class TestFilemerge(unittest.TestCase):
    def setUp(self):
        self._parser = OptionParser()
        self._options_dict = dict.fromkeys(OPTIONS_KEYS)
        self._options_dict.update({"topic": "foo1",
                                   "input_prefix": "foo2",
                                   "output_prefix": "foo3",
                                   "queue": "foo4"})

    def test_getpaths_fromymd_day(self):
        year, month, day = 2015, 02, 12
        input_prefix = "foo"
        expected = [('d_20150212-0000', 'foo/d_20150212*')]
        returned = fm.getpaths_fromymd(input_prefix, year, month, day)
        self.assertEqual(expected, returned)

    def test_getpaths_fromymd_month(self):
        year, month = 2015, 02
        input_prefix = "foo"
        expected = []
        for dd in range(1, 1 + monthrange(year, month)[1]):
            dirname = "d_%d%02d%02d" % (year, month, dd)
            path = "%s/%s*" % (input_prefix, dirname)
            expected.append(("%s-0000" % dirname, path))
        returned = fm.getpaths_fromymd(input_prefix, year, month)
        self.assertEqual(expected, returned)

    def test_getpaths_fromymd_year(self):
        year = 2015
        input_prefix = "foo"
        expected = []
        for mm in range(1, 13):
            for dd in range(1, 1 + monthrange(year, mm)[1]):
                dirname = "d_%d%02d%02d" % (year, mm, dd)
                path = "%s/%s*" % (input_prefix, dirname)
                expected.append(("%s-0000" % dirname, path))
        returned = fm.getpaths_fromymd(input_prefix, year)
        self.assertEqual(expected, returned)

    def test_getpaths_fromdir(self):
        input_prefix = "foo"
        dirname = "bar"
        path = "%s/%s*/*" % (input_prefix, dirname)
        expected = [(dirname, path)]
        returned = fm.getpaths_fromdir(input_prefix, dirname)
        self.assertEqual(expected, returned)

    def test_getpaths_fromfile(self):
        input_prefix = "foo"
        dirs = ["d_201605%02d-0000" % i for i in range(1, 31)]
        dirs += ["\\n"] * 3 # add some newlines
        expected = [(dname, "%s/%s*" % (input_prefix, dname)) for dname in dirs]
        with tempfile.TemporaryFile(dir=".") as fh:
            fh.write("\n".join(dirs))
            fh.seek(0)
            returned = fm.getpaths_fromfile(input_prefix, fh)
        self.assertEqual(expected, returned)

    def test_getpaths_fromwindow(self):
        input_prefix = "foo"
        start_date = datetime.date(2016, 5, 23)
        window = 10
        expected = list()
        dt = start_date
        for _ in range(window):
            dt -= datetime.timedelta(days=1)
            dirname = "d_%d%02d%02d" % (dt.year, dt.month, dt.day)
            path = "%s/%s*" % (input_prefix, dirname)
            expected.append(("%s-0000" % dirname, path))
        returned = fm.getpaths_fromwindow(input_prefix, window, start_date)
        self.assertEqual(expected, returned)

    def test_getpaths_fromlookback(self):
        input_prefix = "foo"
        start_date = datetime.date(2016, 5, 23)
        lookback = 30
        merge_day = start_date - datetime.timedelta(days=lookback)
        dirname = "d_%d%02d%02d" % (merge_day.year, merge_day.month, merge_day.day)
        path = "%s/%s*" % (input_prefix, dirname)
        expected = [("%s-0000" % dirname, path)]
        returned = fm.getpaths_fromlookback(input_prefix, lookback, start_date)
        self.assertEqual(expected, returned)

    def test_get_compression_codec(self):
        CODECS = {
            "gzip": "org.apache.hadoop.io.compress.GzipCodec",
            "bzip": "org.apache.hadoop.io.compress.BZip2Codec",
            "lzo": "com.hadoop.compression.lzo.LzopCodec",
            "snappy": "org.apache.hadoop.io.compress.SnappyCodec"
        }

        CODEC_NAMES = CODECS.keys()
        expected = [CODECS.get(cn, "") for cn in CODEC_NAMES]
        returned = [fm.get_compression_codec(cn) for cn in CODEC_NAMES]
        self.assertEqual(expected, returned)

    @mock.patch("__builtin__.str")
    def test_get_compression_codec_exception(self, mock_str):
        mock_str.side_effect = KeyError()
        with self.assertRaises(KeyError):
            fm.get_compression_codec("foo")

    @mock.patch("filemerge.filemerge.OptionParser.add_option")
    def test_add_options(self, mock_add_options):
        _parser = OptionParser()
        calls = [
            mock.call("-y", "--year",
                      dest="year", action="store",
                      help="Year for the merge"),
            mock.call("-m", "--month",
                      dest="month", action="store",
                      help="Month for the merge"),
            mock.call("-d", "--day",
                      dest="day", action="store",
                      help="Day for the merge"),
            mock.call("-D", "--directory",
                      dest="directory", action="store",
                      help="Directory containing files to merge"),
            mock.call("-f", "--file",
                      dest="file", action="store",
                      help="File containing list of input directories"),
            mock.call("-w", "--window",
                      dest="window", action="store",
                      help="Window in days (merge for the past *n* days"),
            mock.call("-l", "--lookback",
                      dest="lookback", action="store",
                      help="Lookback period (merge for 1 day *n* days prior)"),
            mock.call("-t", "--topic",
                      dest="topic", action="store",
                      help="Topic for the merge"),
            mock.call("-i", "--input-prefix",
                      dest="input_prefix", action="store",
                      help="Input directory prefix"),
            mock.call("-o", "--output-prefix",
                      dest="output_prefix", action="store",
                      help="Output directory prefix"),
            mock.call("-n", "--num-reducers",
                      dest="num_reducers", action="store",
                      help="Number of reducers"),
            mock.call("-c", "--codec",
                      dest="codec", action="store",
                      help="Compression codec to use"),
            mock.call("-q", "--queue",
                      dest="queue", action="store",
                      help="Mapreduce job queue"),
            mock.call("-r", "--dry-run",
                      dest="dry_run", action="store_true", default=False,
                      help="Dry run; create, but dont execute the Pig script")
        ]

        fm.add_options(_parser)
        mock_add_options.assert_has_calls(calls, any_order=False)

    def test_check_options_missing_options(self):
        self._options_dict.update({"topic": None})
        _options = Bunch(**self._options_dict)
        with self.assertRaises(fm.MissingRequiredOptionsException):
            mode = fm.check_options(self._parser, _options)

    def test_check_options_missing_source(self):
        _options = Bunch(**self._options_dict)
        with self.assertRaises(fm.InvalidSourceException):
            mode = fm.check_options(self._parser, _options)

    def test_check_options_sources(self):
        all_sources = ["year", "file", "directory", "window", "lookback"]
        for src in all_sources:
            self._options_dict = dict.fromkeys(OPTIONS_KEYS)
            self._options_dict.update({"topic": "foo1",
                                       "input_prefix": "foo2",
                                       "output_prefix": "foo3",
                                       "queue": "foo4",
                                       src: "foo"})
            _options = Bunch(**self._options_dict)
            mode = fm.check_options(self._parser, _options)
            self.assertEqual(mode, src)

    def test_check_options_too_many_sources(self):
        all_sources = ["year", "file", "directory", "window", "lookback"]
        for src in all_sources:
            self._options_dict.update({src: "foo4"})
        _options = Bunch(**self._options_dict)

        with self.assertRaises(fm.InvalidSourceException):
            mode = fm.check_options(self._parser, _options)

    @mock.patch("filemerge.filemerge.getpaths_fromymd")
    def test_get_paths_ymd(self, mock_getpaths_fromymd):
        src_keys = ["year", "month", "day"]
        for src in src_keys:
            self._options_dict.update({src: "1"})
        _options = Bunch(**self._options_dict)

        paths = fm.getpaths(_options, "year")
        mock_getpaths_fromymd.assert_called_with(_options.input_prefix,
                                                 int(_options.year),
                                                 int(_options.month),
                                                 int(_options.day))

    @mock.patch("filemerge.filemerge.getpaths_fromymd")
    def test_get_paths_ym(self, mock_getpaths_fromymd):
        src_keys = ["year", "month"]
        for src in src_keys:
            self._options_dict.update({src: "1"})
        _options = Bunch(**self._options_dict)
        paths = fm.getpaths(_options, "year")
        mock_getpaths_fromymd.assert_called_with(_options.input_prefix,
                                                 int(_options.year),
                                                 int(_options.month),
                                                 None)

    @mock.patch("filemerge.filemerge.getpaths_fromymd")
    def test_get_paths_y(self, mock_getpaths_fromymd):
        src_keys = ["year"]
        for src in src_keys:
            self._options_dict.update({src: "1"})
        _options = Bunch(**self._options_dict)

        paths = fm.getpaths(_options, "year")
        mock_getpaths_fromymd.assert_called_with(_options.input_prefix,
                                                 int(_options.year),
                                                 None, None)

    @mock.patch("filemerge.filemerge.getpaths_fromdir")
    def test_get_paths_fromdir(self, mock_getpaths_fromdir):
        src_keys = ["directory"]
        for src in src_keys:
            self._options_dict.update({src: "foo"})
        _options = Bunch(**self._options_dict)

        paths = fm.getpaths(_options, "directory")
        mock_getpaths_fromdir.assert_called_with(_options.input_prefix,
                                                 _options.directory)

    @mock.patch("filemerge.filemerge.getpaths_fromfile")
    def test_get_paths_fromfile(self, mock_getpaths_fromfile):
        src_keys = ["file"]
        for src in src_keys:
            self._options_dict.update({src: "foo"})
        _options = Bunch(**self._options_dict)

        with mock.patch("__builtin__.open", create=True) as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            paths = fm.getpaths(_options, "file")
            mock_getpaths_fromfile.assert_called_with(_options.input_prefix,
                                                      mock_open().__enter__())

    @mock.patch("filemerge.filemerge.getpaths_fromwindow")
    def test_get_paths_fromwindow(self, mock_getpaths_fromwindow):
        src_keys = ["window"]
        for src in src_keys:
            self._options_dict.update({src: "20"})
        _options = Bunch(**self._options_dict)

        with mock.patch("filemerge.filemerge.datetime.datetime") as mock_dt:
            mock_dt.now.date.return_value = "foo"
            paths = fm.getpaths(_options, "window")
            mock_getpaths_fromwindow.assert_called_with(_options.input_prefix,
                                                        int(_options.window),
                                                        mock_dt.now().date())

    @mock.patch("filemerge.filemerge.getpaths_fromlookback")
    def test_get_paths_fromlookback(self, mock_getpaths_fromlookback):
        src_keys = ["lookback"]
        for src in src_keys:
            self._options_dict.update({src: "20"})
        _options = Bunch(**self._options_dict)
        paths = fm.getpaths(_options, "lookback")
        mock_getpaths_fromlookback.assert_called_with(_options.input_prefix,
                                                      int(_options.lookback))

    def test_get_paths_exception(self):
        _options_dict = dict.fromkeys(OPTIONS_KEYS)
        _options = Bunch(**_options_dict)
        with self.assertRaises(RuntimeError) as rte:
            paths = fm.getpaths(_options, "foo")

    def test_materialize(self):
        template = "foo=@FOO, bar=@BAR"
        subs = {"@FOO": 1, "@BAR": 2}
        expected = "foo=1, bar=2"
        returned = fm.materialize(template, subs)
        self.assertEqual(expected, returned)

    @mock.patch("filemerge.filemerge.sp.check_call")
    def test_runpig(self, mock_check_call):
        script_path = "/path/to/script"
        result = fm.runpig(script_path)
        cmd = "pig -f %s" % script_path
        mock_check_call.assert_called_with(cmd.split(" "))

    @mock.patch("__builtin__.open")
    @mock.patch("filemerge.filemerge.materialize")
    @mock.patch("filemerge.filemerge.get_compression_codec")
    @mock.patch("filemerge.filemerge.OptionParser")
    @mock.patch("filemerge.filemerge.add_options")
    @mock.patch("filemerge.filemerge.check_options")
    @mock.patch("filemerge.filemerge.getpaths")
    @mock.patch("filemerge.filemerge.runpig")
    def test_main_dry_run(self,
                          mock_runpig,
                          mock_getpaths,
                          mock_check_options,
                          mock_add_options,
                          mock_option_parser,
                          mock_get_comp_codec,
                          mock_materialize,
                          mock_open):
        _options_dict = {
            "month": "08",
            "topic": "topicfoo",
            "directory": "dirfoo",
            "file": "filefoo",
            "year": "2016",
            "day": "24",
            "dry_run": False,
            "input_prefix": "/path/to/data",
            "lookback": "20",
            "queue": "hadoop_queue",
            "window": "10",
            "output_prefix": "/path/to/output",
            "num_reducers": "10",
            "codec": "lzo"
        }
        _options = Bunch(**_options_dict)
        input_paths = [('d_20160804-0000', 'foo/d_20160804*')]
        mock_getpaths.return_value = input_paths
        for dirname, ipath in input_paths:
            substitutions = {
                "@OUTPUT_PATH": os.path.join(_options.output_prefix, dirname),
                "@INPUT_PATH": ipath,
                "@NUM_REDUCERS": _options_dict["num_reducers"],
                "@SET_COMPRESSION_ENABLED": "set output.compression.enabled true",
                "@SET_COMPRESSION_CODEC": "set output.compression.codec com.hadoop.compression.lzo.LzopCodec",
                "@QUEUE": _options.queue
            }

        parser = mock_option_parser.return_value
        parser.parse_args.return_value = (_options, [])
        mock_check_options.return_value = "ymd"
        mock_get_comp_codec.return_value = "com.hadoop.compression.lzo.LzopCodec"
        mock_materialize.return_value = "materialized_foo"
        tf = make_tempfile()
        mock_open.return_value.__enter__.return_value = tf

        fm.main()

        mock_add_options.assert_called_with(parser)
        parser.parse_args.assert_called_with()
        mock_check_options.assert_called_with(parser, _options)
        mock_getpaths.assert_called_with(_options, mode="ymd")
        mock_get_comp_codec.assert_called_with(_options_dict["codec"])
        mock_materialize.assert_called_with(fm.PIG_TEMPLATE, substitutions)
        filename = os.path.join("scripts", "%s-%s.pig" %(_options.topic, dirname))
        mock_runpig.assert_called_with(filename)

        mock_runpig.side_effect = sp.CalledProcessError(1, "foo")
        with self.assertRaises(sp.CalledProcessError):
            fm.main()

        mock_runpig.side_effect = Exception()
        with self.assertRaises(Exception):
            fm.main()

