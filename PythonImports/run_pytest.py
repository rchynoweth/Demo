# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import pytest
import sys

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main(["test_simple_math.py", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------


