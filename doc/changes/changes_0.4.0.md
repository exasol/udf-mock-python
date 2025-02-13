# UDF Mock Python 0.4.0, released 2025-02-13

Code name: Dependency update on top of 0.3.0

## Summary

The release replaces dill with inspect for extracting the code from function,
which solves the compatibility issue with the localstack.  


### Refactorings

* #61: Remove the dependency on dill.
