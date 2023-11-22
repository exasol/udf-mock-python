# UDF Mock Python 0.1.0, released 2023.11.22

Code name: Initial Release

## Summary

This is the initial release of the UDF Mock Python which provides a mock runner for Python3 UDFs which allows you to
test your UDFs locally without a database.

### Features

  - #1: Initial Commit
  - #3: Relax python version to >=3.6.0
  - #7: Make Group work with Iterable and IterableWithSize
  - #27: Added getitem method and test
  - #33: Split MockContext into Standalone and multi-group
  - #40: Added pypi release workflow

### Bugs

  - #30: Correction MockContextRunWrapper for variadic input access
  - #34: Fixed start_col variable
  - #36: Fixed validation of column names for variadic input
  

### Refactorings

  - #5: Update dependencies
  - #8: Extract Interface from MockContext and fix availability of some Context functions
  - #10: Remove dephell and replace it with poetry
  - #29: Add checks for parameter of get_dataframe
  - #22: Updated pandas, numpy dependency, added lapack to github actions
  - #26: Corrected variable names
  - #24: Upgrade python version to 3.8
  - #21: Changed mentions of master to main
  - #41: Removed setup.py 

