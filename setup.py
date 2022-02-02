# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['exasol_udf_mock_python']

package_data = \
{'': ['*']}

install_requires = \
['dill>=0.3.2,<0.4.0', 'pandas>=1.4,<2.0']

setup_kwargs = {
    'name': 'exasol-udf-mock-python',
    'version': '0.1.0',
    'description': 'Mocking framework for Exasol Python UDFs',
    'long_description': '# UDF Mock for Python\n\nThis projects provides a mock runner for Python3 UDFs which allows you\nto test your UDFs locally without a database.\n\n**Note:** This project is in a very early development phase.\nPlease, be aware that the behavior of the mock runner doesn\'t perfectly\nreflect the behaviors of the UDFs inside the database and that the interface still can change.\nIn any case, you need to verify your UDFs with integrations test inside the database.\n\n## Getting started\n\n### Installing via pip\n```\npip install git+https://github.com/exasol/udf-mock-python.git@master\n```\n\n### Installing via poetry\nAdd it to your `tool.poetry.dependencies` or `tool.poetry.dev-dependencies`\n\n```\n[tool.poetry.dev-dependencies]\nexasol-udf-mock-python = { git = "https://github.com/exasol/udf-mock-python.git", branch = "master" }\n...\n```\n\n### How to use the Mock\n\nThe mock runner runs your python UDF in a python environment in which\nno external variables, functions or classes are visble.\nThis means in practice, you can only use things you defined inside your\nUDF and what gets provided by the UDF frameworks,\nsuch as exa.meta and the context for the run function.\nThis includes imports, variables, functions, classes and so on.\n\nYou define a UDF in this framework within in a wrapper function.\nThis wrapper function then contains all necessary imports, functions,\nvariables and classes.\nYou then handover the wrapper function to the `UDFMockExecutor`\nwhich runs the UDF inside if the isolated python environment.\nThe following example shows, how you use this framework:\nThe following example shows the general setup for a test with the Mock:\n\n```\ndef udf_wrapper():\n\n    def run(ctx):\n        return ctx.t1+1, ctx.t2+1.1, ctx.t3+"1"\n\nexecutor = UDFMockExecutor()\nmeta = MockMetaData(\n    script_code_wrapper_function=udf_wrapper,\n    input_type="SCALAR",\n    input_columns=[Column("t1", int, "INTEGER"),\n                   Column("t2", float, "FLOAT"),\n                   Column("t3", str, "VARCHAR(20000)")],\n    output_type="RETURNS",\n    output_columns=[Column("t1", int, "INTEGER"),\n                    Column("t2", float, "FLOAT"),\n                    Column("t3", str, "VARCHAR(20000)")]\n)\nexa = MockExaEnvironment(meta)\nresult = executor.run([Group([(1,1.0,"1"), (5,5.0,"5"), (6,6.0,"6")])], exa)\n```\n\n**Checkout the [tests](tests) for more information about, how to use the Mock.**\n\n## Limitations or missing features\n\nSome of the following limitations are fundamental, other are missing\nfeature and might get removed by later releases:\n\n- Data type checks for outputs are more strict as in real UDFs\n- No support for Import or Export Specification or Virtual Schema adapter\n- No support for dynamic input and output parameters\n- No support for exa.import_script\n- No BucketFS\n- Execution is not isolated in a container\n  - Can access and manipulate the file system of the system running the Mock\n    - UDF inside of the database only can write /tmp to tmp and\n      only see the file system of the script-language container and the mounted bucketfs\n  - Can use all python package available in the system running the Mock\n    - If you use package which are currently not available in the script-language containers,\n      you need create your own container for testing inside of the database\n  - Does not emulate the ressource limitations which get a applied in the database\n- Only one instance of the UDF gets executed\n- No support for Python2, because Python2 is officially End of Life\n',
    'author': 'Torsten Kilias',
    'author_email': 'torsten.kilias@exasol.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/exasol/udf-mock-python',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.8',
}


setup(**setup_kwargs)
