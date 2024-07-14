from typing import Any


from typing import Tuple, Type
from pydantic import AnyUrl
from pydantic_settings import (
    BaseSettings,
    InitSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class MyInitSettingsSource(InitSettingsSource):
    """
    This implementation of InitSettingsSource removes all None values
    from the sources. Othewise is the same as the original implementation.
    """

    def __init__(self, settings_cls: type[BaseSettings], init_kwargs: dict[str, Any]):
        init_kwargs = {k: v for k, v in init_kwargs.items() if v is not None}
        super().__init__(settings_cls, init_kwargs)


class SrealityClientSettings(BaseSettings):
    model_config = SettingsConfigDict()
    base_url: AnyUrl = "https://www.sreality.cz/api/cs/v2/"
    detail_url: AnyUrl = "https://www.sreality.cz/detail/prodej/dum/rodinny/"
    per_page: int = 100

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (MyInitSettingsSource(settings_cls, init_settings.init_kwargs),)
