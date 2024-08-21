from typing import Any

from typing import Tuple, Type, List
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


class MinioClientSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="MINIO_", extra="ignore"
    )
    endpoint: str = "localhost:9000"
    access_key: str  # loaded from env
    secret_key: str  # loaded from env


class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="RABBITMQ_", extra="ignore"
    )
    endpoint: str = "localhost"
    username: str  # loaded from env
    password: str  # loaded from env


class TelegramBotSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="TELEGRAM_", extra="ignore"
    )
    token:str # loaded from env
    queue_name:str = "estates_hits"

class CommuteTimeFeatureSettings(BaseSettings):
    model_config = SettingsConfigDict()
    minutes_per_km: int = 2


class PIDCommuteFeatureEnhancerSettings:
    desired_stop: str = "Smíchovské nádraží"
    stops_path = "all_stops.json"


class PIDClientSettings(BaseSettings):
    url_base: str = "https://pid.cz/wp-admin/admin-ajax.php"
    query: List = [
        ["action", "crwsSearch"],
        ["stop_over", ""],
        ["time", "08:00"],
        ["direct", ""],
        ["maxtransfers", "výchozí"],
        ["transports[]", "metro"],
        ["transports[]", "vlak"],
        ["transports[]", "tram"],
        ["transports[]", "bus"],
        ["transports[]", "trolley"],
        ["transports[]", "privoz"],
        ["transports[]", "lan"],
        ["speed", "high"],
        ["ajax", "true"],
    ]
