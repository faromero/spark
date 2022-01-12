#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import json
import warnings
from typing import (
    cast,
    overload,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from math import ceil

from pyspark.context import SparkContext
from pyspark.sql.viva_utils import *
from pyspark.sql.types import DataType, StructField, StructType, IntegerType, StringType, Row

import pandas as pd

__all__ = ["VIVA"]

class VIVA(object):

    """
    TODO: update this as we go
    VIVA optimizer and execution engine

    :class:`VIVA` is used as::

        # 1. viva.run(dataframe)

    .. versionadded:: 1.3.0
    """
    def __init__(self) -> None:
      # Load in metadata
      self._video_metadata = self._load_video_metadata()

    """
    Load in video metadata.
    Currently assumes the metadata is serialized into a file called videos_ser.bin.
    This can ultimately be hooked up to Redis or similar.
    """
    def _load_video_metadata(self) -> None:
      df = pd.read_pickle(os.path.join(os.path.expanduser('~'), 'videos_ser.bin'))
      return df

    def _optimize(self, query_inp: List[Row]) -> List[Row]:
      print('In optimizer')
      return query_inp

    def _execute(self, column_to_materialize: str, query_plan: List[Row]) -> List[Row]:
      # Fetch video
      bucket_name = 'franky-va-datasets'
      video_name = 'bernie_clip'
      blob_name = os.path.join('tvnews_videos', 'viva_vid', video_name + '.mp4')
      destination_file_name = os.path.join('/tmp', video_name + '.mp4')
      if not fetch_video(bucket_name=bucket_name, source_blob_name=blob_name, destination_file_name=destination_file_name):
        print('Failed to fetch video')
        return []

      # Get FPS
      fps = 30 #ceil(self._video_metadata.loc[self._video_metadata['Name'] == video_name.split('.')[0], 'FPS'].values[0])
      prefix = 'frame_'
      frames_directory = '/tmp/frames_dir'
      if not decode_video(input_video_path=destination_file_name, video_fps=fps, frame_prefix=prefix, destination_directory=frames_directory):
        print('Failed to decode video')
        return []

      # Run inference
      inf_results = run_inference_yolov5(input_frame_directory=frames_directory)

      # Post-process the results
      new_rows = postprocess_results_yolov5(column_to_materialize=column_to_materialize, predictions=inf_results)

      return new_rows

    def _execute_dummy(self, schema: StructType, query_plan: List[Row]) -> List[Row]:
      print('In executor')
      data = [(3, [6,9], True)]
      temp_dict = {}
      for s,d in zip(schema, data[0]):
        temp_dict[s.name] = d
        
      row = Row(**temp_dict)
      print('New row:', row)
      query_plan.append(row)
      return query_plan

    def run(self, schema: str, query_inp: List[Row]) -> List[Row]:
      opt_plan = self._optimize(query_inp)
      exec_result = self._execute(schema, opt_plan)

      return exec_result

