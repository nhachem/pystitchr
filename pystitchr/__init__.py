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

# monkey patching
from .engine import *
from .base.df_transforms import *
from .base.df_checks import *

import pystitchr.base.df_transforms as dft
import pystitchr.base.df_functions as fn

# Transforms
DataFrame.rename_columns = rename_columns
DataFrame.add_columns = add_columns
DataFrame.drop_columns = drop_columns
DataFrame.filter_or = filter_or
DataFrame.filter_op = filter_op
DataFrame.filter_and = filter_and
DataFrame.pivot = pivot
# ToDo: NH need to debug the unpivot code
DataFrame.unpivot = test_unpivot
DataFrame.flatten = flatten
DataFrame.flatten_no_explode = flatten_no_explode
DataFrame.rename_4_parquet = rename_4_parquet
DataFrame.select_list = select_list
DataFrame.select_exclude = select_exclude

# checks and validation
DataFrame.check_numeric_bound = check_numeric_bound
DataFrame.check_upper_bound = check_upper_bound
DataFrame.check_lower_bound = check_lower_bound
DataFrame.check_less_than = check_less_than
DataFrame.check_greater_than = check_greater_than
DataFrame.check_negative_numeric = check_negative_numeric
DataFrame.check_positive_numeric = check_positive_numeric
DataFrame.unique_check = unique_check

# engine.run_pipeline
DataFrame.run_pipeline = engine.run_pipeline
DataFrame.transform0 = engine.transform0

