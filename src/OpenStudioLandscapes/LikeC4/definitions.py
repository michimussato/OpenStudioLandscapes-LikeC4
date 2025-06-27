from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.LikeC4.assets
import OpenStudioLandscapes.LikeC4.constants

assets = load_assets_from_modules(
    modules=[OpenStudioLandscapes.LikeC4.assets],
)

constants = load_assets_from_modules(
    modules=[OpenStudioLandscapes.LikeC4.constants],
)


defs = Definitions(
    assets=[
        *assets,
        *constants,
    ],
)
