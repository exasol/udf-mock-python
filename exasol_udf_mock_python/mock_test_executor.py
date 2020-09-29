from multiprocessing import Lock
from typing import Dict, Any

from exasol_udf_mock_python.mock_context import MockContext
from exasol_udf_mock_python.mock_context_run_wrapper import MockContextRunWrapper
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment


def _wrapped_run(ctx, exa, runfunc):
    wrapped_ctx = MockContextRunWrapper(ctx, exa.meta.input_type, exa.meta.output_type)
    if exa.meta.input_type == "SET":
        if exa.meta.output_type == "EMIT":
            ctx.emit(runfunc(wrapped_ctx))
        else:
            runfunc(wrapped_ctx)
    else:
        if exa.meta.output_type == "RETURNS":
            while (True):
                ctx.emit(runfunc(wrapped_ctx))
                if not ctx.next():
                    break
        else:
            while (True):
                runfunc(wrapped_ctx)
                if not ctx.next():
                    break


class MockTestExecutor:
    _lock = Lock()

    def _exec_run(self, exec_globals: Dict[str, Any], ctx: MockContext):
        codeObject = compile("__wrapped_run(__mock_test_executor_ctx, exa, run)", 'exec_run', 'exec')
        exec_locals = {}
        exec_globals["__mock_test_executor_ctx"] = ctx
        exec_globals["__wrapped_run"] = _wrapped_run
        exec(codeObject, exec_globals, exec_locals)

    def _exec_cleanup(self, exec_globals: Dict[str, Any]):
        codeObject = compile("cleanup()", 'exec_cleanup', 'exec')
        exec(codeObject, exec_globals)

    def _exec_init(self, exa_environment: MockExaEnvironment) -> Dict[str, Any]:
        codeObject = compile(exa_environment.meta.script_code, 'udf', 'exec')
        exec_globals = {"exa": exa_environment}
        exec(codeObject, exec_globals)
        return exec_globals

    def run(self, inputs, exa_environment: MockExaEnvironment):
        with self._lock:
            ctx = MockContext(inputs, exa_environment.meta)
            exec_globals = self._exec_init(exa_environment)
            try:
                self._exec_run(exec_globals, ctx)
            finally:
                if "cleanup" in exec_globals:
                    self._exec_cleanup(exec_globals)
            return ctx._outputs
