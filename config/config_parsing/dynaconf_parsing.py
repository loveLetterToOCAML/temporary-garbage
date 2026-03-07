from dynaconf import Dynaconf


def parse_config():
    pass

settings = Dynaconf(
    envvar_prefix="MYPROGRAM",
    settings_files=["settings.toml", ".secrets.toml"],
    environments=True,
    load_dotenv=True,
    env_switcher="MYPROGRAM_ENV",
    **more_options
)

DefaultDynaconfConfigParser = Options(
    env=cp.defaultColoration,
    environments=cp.parseEnvironment,
    envvar_prefix=cp.environmentPrefix,
    #env_switcher=None,
    root_path=cp.rootPath,
    setting_file=cp.defaultConfigName,
    #secrets=None,
    load_dotenv=cp.supplementalDotenvFile,
)
