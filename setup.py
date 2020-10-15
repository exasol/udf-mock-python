
# -*- coding: utf-8 -*-

# DO NOT EDIT THIS FILE!
# This file has been autogenerated by dephell <3
# https://github.com/dephell/dephell

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


import os.path

readme = ''
here = os.path.abspath(os.path.dirname(__file__))
readme_path = os.path.join(here, 'README.rst')
if os.path.exists(readme_path):
    with open(readme_path, 'rb') as stream:
        readme = stream.read().decode('utf8')


setup(
    long_description=readme,
    name='exasol-udf-mock-python',
    version='0.1.0',
    description='Mocking framework for Exasol Python UDFs',
    python_requires='>=3.6.1',
    project_urls={"homepage": "https://github.com/exasol/udf-mock-python", "repository": "https://github.com/exasol/udf-mock-python"},
    author='Torsten Kilias',
    author_email='torsten.kilias@exasol.com',
    license='MIT',
    keywords='exasol udf mock testing',
    packages=['exasol_udf_mock_python'],
    package_dir={"": "."},
    package_data={},
    install_requires=['dill==0.*,>=0.3.2', 'pandas==1.*,>=1.1.3'],
    extras_require={"dev": ["pytest==6.*,>=6.1.1", "pytest-cov==2.*,>=2.10.1"]},
)
