

class EffectfulFilerServerMultibackend():

    def __init__(self, *server_params):
        pass


current_backends = ContextVarWrapper[list[EffectfulBackend[HashType, BackendFailure]]]('current_backends')
