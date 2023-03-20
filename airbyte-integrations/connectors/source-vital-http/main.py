#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_vital_http import SourceVitalHttp

if __name__ == "__main__":
    source = SourceVitalHttp()
    launch(source, sys.argv[1:])
