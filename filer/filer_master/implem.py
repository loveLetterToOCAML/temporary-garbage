import contextlib


base_location = 'filerMasterBase'

# ensure_filer_master_config -> RootConfig.ExecutionSystem.Filer.FilerMaster

@contextlib.contextmanager
def ensure_filer_master_context(location: str | None = None):
    with (
        ensure_filer_master_config(),
        ensure_logger_context(location or base_location) as lc,
        ensure_persistance_context(FilerMasterModel),
        ensure_valid_authority()
    ):
        try:
            yield fc
        except Exception as e:
            lc.exception(e)


UnprivilegedFilerMasterIntent = GetFilers | ResolveContent | SearchHashPrefix
PrivilegedFilerMasterIntent = RegisterNewFiler | UnregisterFiler | GetFilers
