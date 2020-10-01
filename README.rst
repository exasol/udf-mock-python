.. role:: raw-html-m2r(raw)
   :format: html


**This project is in a very early development phase.\ :raw-html-m2r:`<br>`
Please, be aware that the behavior of the mock runner doesn't perfectly\ :raw-html-m2r:`<br>`
reflect the behaviors of the UDFs inside the database. In any case,\ :raw-html-m2r:`<br>`
you need to verify your UDFs with integrations test inside the database.**

This projects provides a mock runner for Python3 UDFs which allows you\ :raw-html-m2r:`<br>`
to test your UDFs locally without a database.
The mock runner runs your python UDF in a python environment in which\ :raw-html-m2r:`<br>`
no external variables, functions or classes are visable.
This means in practice, you can only use things you defined inside your\ :raw-html-m2r:`<br>`
UDF and what gets provided by the UDF frameworks,\ :raw-html-m2r:`<br>`
such as exa.meta and the context for the run function.
This includes imports, variables, functions, classes and so on.
You define a UDF in this framework within in a wrapper function.
This wrapper function then contains all necassary imports, functions,\ :raw-html-m2r:`<br>`
variables and classes.
You then handover the wrapper function to the ``UDFMockExecutor``\ :raw-html-m2r:`<br>`
which runs the UDF inside if the isolated python environment.
The following example shows, how you use this framework:

.. code-block::

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

Limitations:


* Currently, data type checks for outputs are more strict as in real UDFs
* Currently, no support for Import or Export Specification or Virtual Schema adapter
* Currently, no support for dynamic input and output parameters
* No BucketFS access
* Is not isolated in a container, only the python codes runs in a isolated environment, which means not external variables, functions or classes are visiable inside the UDF
* No support for Python2, because Python2 is officially End of Life and only get served by some Linux distributions with very critical fixes
