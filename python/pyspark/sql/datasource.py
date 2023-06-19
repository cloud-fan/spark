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
"""
Data source related classes and functions.
"""
from typing import Any, Dict, Iterator, Optional

from pyspark.sql.types import StructType


class DataSourceReader:

    def __init__(self, options: Dict) -> None:
        self.options = options

    def getPartitions(self) -> Iterator[Any]:
        """Returns the partition when reading the data."""
        ...

    def read(self, partition: Any) -> Iterator[Any]:
        """Returns an iterator of row for the given partition."""
        ...


class DataSourceWriter:

    def __init__(self, schema, options: Dict) -> None:
        self.schema = schema
        self.options = options

    def write(self, *args) -> None:
        """Writes out the input row."""
        ...

    def commit(self) -> Any:
        """Returns the commit message after writing the data."""
        ...


class DataSource:

    def __init__(self, schema: Optional[StructType] = None, options: Dict = None):
        self.schema = schema
        self.options = options

    def getSchema(self) -> StructType:
        raise NotImplementedError()

    def getReader(self) -> DataSourceReader:
        raise NotImplementedError()

    def getWriter(self) -> DataSourceWriter:
        raise NotImplementedError()
