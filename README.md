# UDF Mock for Python

This projects provides a mock runner for Python3 UDFs which allows you
to test your UDFs locally without a database.

**Note:** This project is in a very early development phase.
Please, be aware that the behavior of the mock runner doesn't perfectly
reflect the behaviors of the UDFs inside the database and that the interface still can change.
In any case, you need to verify your UDFs with integrations test inside the database.

## Getting started

### Installing via pip
```
pip install git+https://github.com/exasol/exasol-udf-mock-python.git@master
```

### Installing via poetry
Add it to your `tool.poetry.dependencies` or `tool.poetry.dev-dependencies`

```
[tool.poetry.dev-dependencies]
exasol-udf-mock-python = { git = "https://github.com/exasol/exasol-udf-mock-python.git", branch = "master" }
...
```

### How to use the Mock

The mock runner runs your python UDF in a python environment in which
no external variables, functions or classes are visble.
This means in practice, you can only use things you defined inside your
UDF and what gets provided by the UDF frameworks,
such as exa.meta and the context for the run function.
This includes imports, variables, functions, classes and so on.

You define a UDF in this framework within in a wrapper function.
This wrapper function then contains all necessary imports, functions,
variables and classes.
You then handover the wrapper function to the `UDFMockExecutor`
which runs the UDF inside if the isolated python environment.
The following example shows, how you use this framework:
The following example shows the general setup for a test with the Mock:

```
def udf_wrapper():

    def run(ctx):
        return ctx.t1+1, ctx.t2+1.1, ctx.t3+"1"

executor = UDFMockExecutor()
meta = MockMetaData(
    script_code_wrapper_function=udf_wrapper,
    input_type="SCALAR",
    input_columns=[Column("t1", int, "INTEGER"),
                   Column("t2", float, "FLOAT"),
                   Column("t3", str, "VARCHAR(20000)")],
    output_type="RETURNS",
    output_columns=[Column("t1", int, "INTEGER"),
                    Column("t2", float, "FLOAT"),
                    Column("t3", str, "VARCHAR(20000)")]
)
exa = MockExaEnvironment(meta)
result = executor.run([Group([(1,1.0,"1"), (5,5.0,"5"), (6,6.0,"6")])], exa)
```

Checkout the [tests](tests) for more information about, how to use the Mock.

## Limitations

Some of the following limitations are fundamental, other might get removed by later releases:

- Data type checks for outputs are more strict as in real UDFs
- No support for Import or Export Specification or Virtual Schema adapter
- No support for dynamic input and output parameters
- No BucketFS
- Execution is not isolated in a container
  - Can access and manipulate the file system of the system running the Mock
    - UDF inside of the database only can write /tmp to tmp and
      only see the file system of the script-language container and the mounted bucketfs
  - Can use all python package available in the system running the Mock
    - If you use package which are currently not available in the script-language containers,
      you need create your own container for testing inside of the database
  - Does not emulate the ressource limitations which get a applied in the database
- Only one instance of the UDF gets executed
- No support for Python2, because Python2 is officially End of Life

