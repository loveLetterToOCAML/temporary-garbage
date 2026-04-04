
import contextlib


base_location = 'filerClientBase'

# ensure_filer_client_config -> RootConfig.ExecutionSystem.Filer.FilerClient

@contextlib.contextmanager
def ensure_filer_client_context(location: str | None = None):
    with (
        ensure_filer_client_config(),
        ensure_logger_context(location or base_location) as lc,
        ensure_persistance_context(BasicFilerClientModel),
        filer_master_context() as fmc,  # in degraded mode one could switch off this context
        filer_registries_context() as frcs,
        filer_client_context(
            filerMasterContext = fmc,
            filerRegistriesContext = frcs,
        ) as fc
    ):
        try:
            yield fc
        except Exception as e:
            lc.exception(e)

def forward_intent(intent: FilerClientIntent):
    fc = filer_client_contextvar.get()
    resolved_or_error = fc.filerMasterContext.who_has(intent.ulid or intent.hash)
    if error := error_of_result(resolved_or_error):
        return error

    resolved_registry_id = resolved_or_error.registryId
    resolved_registry_or_error = fc.filerRegistriesContext.ensure_reachable(resolved_registry_id)
    if error := error_of_result(resolved_registry_or_error):
        return error
    resolved_registry = resolved_registry_or_error

    pc = persistance_contextvar.get()
    pc.save(
        BasicFilerClientModel.SuccessLog(
            intentType=intent.type,
            contentIdentifier=intent.ulid or intent.hash
        ) # auto date
    )
    return resolved_registry.process_intent(intent)


def perform_within_context(intent: FilerClientIntent):
    match intent:
        case GetContentIntent():
            result = download_content(intent)
        case UploadContentIntent():
            result = upload_content(intent)
        case GetContentSizeIntent() | GetContentULIDForHashIntent() | GetContentHashForULIDIntent() | \
            CheckContentForHashAndULIDIntent() | DeleteContentIntent():
            result = forward_intent(intent)
        case _:
            raise BadFilerClientIntent()
