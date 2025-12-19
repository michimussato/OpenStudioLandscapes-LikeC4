from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.LikeC4.assets

assets = load_assets_from_modules(
    modules=[OpenStudioLandscapes.LikeC4.assets],
)


defs = Definitions(
    assets=[
        *assets,
    ],
)
