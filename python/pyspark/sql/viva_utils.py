
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
import glob
import subprocess as sp
from itertools import islice
from google.cloud import storage

import torch
from torch.utils.data import Dataset
from torchvision import datasets, models, transforms
from torchvision.datasets.folder import default_loader  # private API

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

from pyspark.sql.types import DataType, StructField, StructType, IntegerType, StringType, Row

import numpy as np

from typing import (
    cast,
    overload,
    Any,
    Callable,
    Iterable,
    List,
    Dict,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

if TYPE_CHECKING:
    from pyspark.sql.context import SQLContext
    from pyspark.sql.dataframe import DataFrame

"""
Fetch video.
Return True if successful, false otherwise
"""
def fetch_video(bucket_name: str, source_blob_name: str, destination_file_name: str) -> bool:
  if os.path.exists(destination_file_name):
    print('{} already downloaded'.format(destination_file_name))
    return True

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(source_blob_name)
  blob.download_to_filename(destination_file_name)

  print(
      "Downloaded storage object {} from bucket {} to local file {}.".format(
          source_blob_name, bucket_name, destination_file_name
      )
  )

  return True

"""
Decode video.
Return True if successful, false otherwise
"""
def decode_video(input_video_path: str, video_fps: str, frame_prefix: str, destination_directory: str) -> bool:
  if os.path.exists(destination_directory):
    dir = os.listdir(destination_directory)
    # Check if directory is empty
    if len(dir) != 0:
      print('{} already decoded'.format(input_video_path))
      return True
  else:
    os.mkdir(destination_directory)

  # Build decode command
  output_prefix = os.path.join(destination_directory, frame_prefix + '%04d.jpg')
  ffmpeg_cmd = 'ffmpeg -i {input} -r {fps} -vsync cfr {prefix}'.format(
      input=input_video_path, fps=video_fps, prefix=output_prefix)

  #sp.call(ffmpeg_cmd.split(), stderr=sp.DEVNULL, stdout=sp.DEVNULL)
  sp.call(ffmpeg_cmd.split())

  return True

"""
Run inference using ResNet50.
TODO: optimize this a lot.
Return results as a map of {<frame_id>:label}.
"""
def run_inference_resnet50(input_frame_directory: str) -> List[Any]:
  use_cuda = torch.cuda.is_available()
  device = torch.device("cuda" if use_cuda else "cpu")

  model = models.resnet50(pretrained=True)
  model.eval()

  labels_fd = open(os.path.join(os.path.expanduser('~'), 'imagenet_labels.txt'))
  labels = [i for i in labels_fd]
  labels_fd.close()

  class ImageDataset(Dataset):
    def __init__(self, paths, transform=None):
      self.paths = paths
      self.transform = transform
    def __len__(self):
      return len(self.paths)
    def __getitem__(self, index):
      image = default_loader(self.paths[index])
      if self.transform is not None:
        image = self.transform(image)
      return image

  transform = transforms.Compose([
  transforms.Resize(224),
  transforms.CenterCrop(224),
  transforms.ToTensor(),
  transforms.Normalize(mean=[0.485, 0.456, 0.406],
                     std=[0.229, 0.224, 0.225])
  ])

  paths = glob.glob(os.path.join(input_frame_directory, '*.jpg'))
  images = ImageDataset(paths, transform=transform)
  loader = torch.utils.data.DataLoader(images, batch_size=8, num_workers=8)
  model.to(device)
  all_predictions = []
  with torch.no_grad():
    for batch in loader:
      predictions = list(model(batch.to(device)).cpu().numpy())
      for prediction in predictions:
        indices = np.argsort(prediction)[::-1]
        top_label = labels[indices[0]]
        all_predictions.append(top_label)

  return all_predictions

"""
Run inference using YOLOv5.
TODO: optimize this a lot.
Return results as a map of {<frame_id>:[pandas_dataframe]}.
Each pandas_dataframe contains: xmin, ymin, xmax, ymax, confidence, class, name
"""
def run_inference_yolov5(input_frame_directory: str) -> Dict[str, Any]:
  model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)

  all_outputs = {}

  # Create batches
  def batch(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())

  batch_size = 8
  all_imgs = glob.glob(os.path.join(input_frame_directory, '*.jpg'))
  batches = list(batch(all_imgs, batch_size))

  for b in batches:
    results = model(list(b))
    pd = results.pandas().xyxy

    for indiv_img,indiv_res in zip(b,pd):
      all_outputs[indiv_img] = indiv_res

  return all_outputs

"""
Post process results and build the Row(s) to be returned.
Return list of Rows
"""
def postprocess_results_yolov5(column_to_materialize: str, predictions: Dict[str, Any]) -> List[Any]:

  final_rows = []
  for frame_id, pred in predictions.items():
    next_row_map = {}
    for _, row in pred.iterrows():
      next_bounding_box = [row['xmin'], row['xmax'], row['ymin'], row['ymax']]
      next_label = row['name']
      next_row_map = {'frame_id': frame_id, 'bounding_box': next_bounding_box, column_to_materialize: next_label}
      final_rows.append(Row(**next_row_map))
   
  return final_rows

