


class ExecutionContext(BaseModel):
    threadId: int
    processId: int
    fiberId: int
    executionSystem: ExecutionSystem


CurrentExecutionContext = register_dependent_init(
    'current_execution_context', initialize_execution_context, current_execution_context,
    depends_on=[]
)
