from pathlib import Path

from exasol.toolbox.config import BaseConfig

ROOT_DIR = Path(__file__).parent

from pydantic import computed_field


class Config(BaseConfig):
    @computed_field  # type: ignore[misc]
    @property
    def has_documentation(self) -> bool:
        """
        Indicates that the project serves Sphinx-based documentation. With a few
        exceptions, this should be the case for most projects.

        This needs to be overridden as Sphinx-documentation is not setup. This will
        be addressed in:
            https://github.com/exasol/udf-mock-python/issues/38
        """
        return False

    @computed_field  # type: ignore[misc]
    @property
    def source_code_path(self) -> Path:
        """
        Path to the source code of the project.

        This needs to be overridden due to a custom directory setup. This will be
        addressed in:
            https://github.com/exasol/udf-mock-python/issues/80
        """
        return self.root_path / self.project_name


PROJECT_CONFIG = Config(
    project_name="exasol_udf_mock_python",
    root_path=ROOT_DIR,
    python_versions=("3.10", "3.11", "3.12", "3.13"),
)
