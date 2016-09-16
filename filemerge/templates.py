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

PIG_TEMPLATE = \
    '''
    set mapreduce.job.queuename @QUEUE
    set default_parallel @NUM_REDUCERS
    @SET_COMPRESSION_ENABLED
    @SET_COMPRESSION_CODEC
    set pig.splitCombination false

    rmf @OUTPUT_PATH
    A = load '@INPUT_PATH' using PigStorage('\u0001', '-tagFile') AS (filename: chararray,line: chararray);
    B = foreach (group A by filename) generate FLATTEN(A.line);
    store B into '@OUTPUT_PATH';
    '''

DATE_TEMPLATE = "d_%d%02d%02d"
