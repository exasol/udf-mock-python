# 0.7.0 - 2025-09-11
## Summary

This release loosens the constraints for dependency `numpy` as python 3.13 is only available for numpy versions >2.

### Refactorings

* #64: Update to poetry 2.1.2
* #66: Loosen constraint for dependency `numpy`

## Dependency Updates

### `main`
* Removed dependency `dill:0.3.8`
* Updated dependency `numpy:1.24.4` to `1.26.4`
* Updated dependency `pandas:1.5.3` to `2.2.3`

### `dev`
* Added dependency `exasol-toolbox:1.9.0`
* Added dependency `pytest:8.3.5`
* Added dependency `pytest-cov:5.0.0`
