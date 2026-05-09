import enum
import pathlib
from typing import List

from OpenStudioLandscapes.engine.config.models import FeatureBaseModel
from pydantic import (
    Field,
    PositiveInt,
)

from OpenStudioLandscapes.LikeC4 import (
    LOGGER,
    dist,
    ASSET_HEADER,
)


class Releases(enum.StrEnum):
    release_v1_46_3 = (
        "https://github.com/likec4/likec4/archive/refs/tags/v1.46.3.tar.gz"
    )


class DockerImages(enum.StrEnum):
    # https://likec4.dev/tooling/docker/
    dockerhub = "docker.io/likec4/likec4"
    github_container_registry = "ghcr.io/likec4/likec4"


class Config(FeatureBaseModel):
    feature_name: str = dist.name

    group_name: str = ASSET_HEADER["group_name"]

    key_prefixes: List[str] = ASSET_HEADER["key_prefix"]

    # https://likec4.dev/tooling/docker/#start-local-web-server

    likec4_docker_image: DockerImages = Field(
        default=DockerImages.dockerhub,
        description="The LikeC4 Docker image.",
        examples=[i.name for i in DockerImages],
    )

    likec4_LIKEC4_DEV_PORT_HOST: PositiveInt = Field(
        default=5173,
        description="The LikeC4 host port.",
        frozen=False,
    )

    likec4_LIKEC4_DEV_PORT_CONTAINER: PositiveInt = Field(
        default=5173,
        description="The LikeC4 container port.",
        frozen=False,
    )

    # (optional) for realtime updates: -p 24678:24678
    likec4_LIKEC4_RT_DEV_PORT_HOST: PositiveInt = Field(
        default=24678,
        description="The LikeC4 realtime update host port.",
        frozen=False,
    )

    likec4_LIKEC4_RT_DEV_PORT_CONTAINER: PositiveInt = Field(
        default=24678,
        description="The LikeC4 realtime update container port.",
        frozen=False,
    )

    # (optional) use init process to correctly handle signals (eg Ctrl+C): --init
    likec4_LIKEC4_CHOKIDAR_USEPOLLING: PositiveInt = Field(
        default=1,
        frozen=False,
    )

    likec4_LIKEC4_CHOKIDAR_INTERVAL: PositiveInt = Field(
        default=200,
        frozen=False,
    )

    likec4_DATA_ROOT: pathlib.Path = Field(
        description="The host side LikeC4 datastore destination.",
        default=pathlib.Path("{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/data"),
    )

    likec4_sources: Releases = Field(
        default=Releases.release_v1_46_3,
        examples=[i.name for i in Releases],
    )

    # EXPANDABLE PATHS
    @property
    def likec4_DATA_ROOT_expanded(self) -> pathlib.Path:
        LOGGER.debug(f"{self.env = }")
        if self.env is None:
            raise KeyError("`env` is `None`.")

        LOGGER.debug(f"Expanding {self.likec4_DATA_ROOT}...")
        ret = pathlib.Path(
            self.likec4_DATA_ROOT.expanduser()  # pylint: disable=E1101
            .as_posix()
            .format(
                **{
                    "FEATURE": self.feature_name,
                    **self.env,
                }
            )
        )
        return ret


if __name__ == "__main__":
    CONFIG_STR = Config.get_docs()
else:
    import yaml

    CONFIG_STR = yaml.dump(
        Config.model_json_schema(mode="serialization"),
    )
