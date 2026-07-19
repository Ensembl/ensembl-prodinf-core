# .. See the NOTICE file distributed with this work for additional information
#    regarding copyright ownership.
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#        http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# Build-time only: kept out of the "ensembl.production.core" namespace package
# so it can't collide with other Ensembl packages installed alongside it.
import os
from pathlib import Path

with open(Path(__file__).parents[1] / 'VERSION') as f:
    _base_version = f.read().strip()

__version__ = os.getenv('CI_COMMIT_TAG', _base_version)
