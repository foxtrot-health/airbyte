#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_outseta_tutorial import SourceOutsetaTutorial

if __name__ == "__main__":
    source = SourceOutsetaTutorial()
    launch(source, sys.argv[1:])
