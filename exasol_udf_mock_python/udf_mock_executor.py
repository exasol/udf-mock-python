from multiprocessing import Lock
from typing import Dict, Any, Iterator, List, Union, Callable, Tuple
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_context import MockContext
from exasol_udf_mock_python.mock_context_run_wrapper import MockContextRunWrapper
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment


def _loop_groups(ctx:MockContext, exa:MockExaEnvironment, runfunc:Callable):
    while ctx.next_group():
        _wrapped_run(ctx, exa, runfunc)


def _wrapped_run(ctx:MockContext, exa: MockExaEnvironment, runfunc: Callable):
    wrapped_ctx = MockContextRunWrapper(
        ctx, exa.meta.input_type, exa.meta.output_type, exa.meta.is_variadic_input)
    if exa.meta.input_type == "SET":
        if exa.meta.output_type == "RETURNS":
            run_with_returns(ctx, runfunc, wrapped_ctx)
        else:
            runfunc(wrapped_ctx)
    else:
        if exa.meta.output_type == "RETURNS":
            while (True):
                run_with_returns(ctx, runfunc, wrapped_ctx)
                if not ctx.next():
                    break
        else:
            while (True):
                runfunc(wrapped_ctx)
                if not ctx.next():
                    break


def run_with_returns(ctx, runfunc, wrapped_ctx):
    result = runfunc(wrapped_ctx)
    if isinstance(result, Tuple):
        ctx.emit(*result)
    else:
        ctx.emit(result)


class UDFMockExecutor:
    _lock = Lock()

    def _exec_run(self, exec_globals: Dict[str, Any], ctx: MockContext):
        codeObject = compile("__loop_groups(__mock_test_executor_ctx, exa, run)", 'exec_run', 'exec')
        exec_locals = {}
        exec_globals["__mock_test_executor_ctx"] = ctx
        exec_globals["__loop_groups"] = _loop_groups
        exec(codeObject, exec_globals, exec_locals)

    def _exec_cleanup(self, exec_globals: Dict[str, Any]):
        codeObject = compile("cleanup()", 'exec_cleanup', 'exec')
        exec(codeObject, exec_globals)

    def _exec_init(self, exa_environment: MockExaEnvironment) -> Dict[str, Any]:
        codeObject = compile(exa_environment.meta.script_code, 'udf', 'exec')
        exec_globals = {"exa": exa_environment}
        exec(codeObject, exec_globals)
        return exec_globals

    def run(self,
            input_groups:Union[Iterator[Group],List[Group]],
            exa_environment: MockExaEnvironment)\
            ->List[Group]:
        with self._lock:
            if isinstance(input_groups,Iterator):
                ctx = MockContext(input_groups, exa_environment.meta)
            elif isinstance(input_groups,List):
                ctx = MockContext(iter(input_groups), exa_environment.meta)
            else:
                raise TypeError(f"{type(input_groups)} for input_groups not supported")
            exec_globals = self._exec_init(exa_environment)
            try:
                self._exec_run(exec_globals, ctx)
            finally:
                if "cleanup" in exec_globals:
                    self._exec_cleanup(exec_globals)
            return ctx.output_groups
