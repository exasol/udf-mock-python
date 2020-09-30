**This project is in a very early development phase.  
Please, be aware that the behavior of the mock runner doesn't perfectly  
reflect the behaviors of the UDFs inside the database. In any case,  
you need to verify your UDFs with integrations test inside the database.**

This projects provides a mock runner for Python3 UDFs which allows you  
to test your UDFs locally without a database.
The mock runner runs your python UDF in a python environment in which  
no external variables, functions or classes are visable.
This means in practice, you can only use things you defined inside your  
UDF and what gets provided by the UDF frameworks,  
such as exa.meta and the context for the run function.
This includes imports, variables, functions, classes and so on.
You define a UDF in this framework within in a wrapper function.
This wrapper function then contains all necassary imports, functions,  
variables and classes.
You then handover the wrapper function to the `UDFMockExecutor`  
which runs the UDF inside if the isolated python environment.
The following example shows, how you use this framework:

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


Limitations:

- Currently, data type checks for outputs are more strict as in real UDFs
- Currently, no support for Import or Export Specification or Virtual Schema adapter
- Currently, no support for dynamic input and output parameters
- No BucketFS access
- Is not isolated in a container, only the python codes runs in a isolated environment, which means not external variables, functions or classes are visiable inside the UDF
- No support for Python2, because Python2 is officially End of Life and only get served by some Linux distributions with very critical fixes

