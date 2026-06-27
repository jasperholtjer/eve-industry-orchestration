"""Resource registration for the orchestration project."""

import os

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.serving_resource import ServingResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "corpus": CorpusResource(
                binary_path=dg.EnvVar("CORPUS_BINARY_PATH"),
                datasets_dir=dg.EnvVar("CORPUS_DATASETS_DIR"),
                sink_path=dg.EnvVar("CORPUS_SINK_PATH"),
            ),
            # Host/user are configurable via SERVING_HOST / SERVING_USER, defaulting
            # to serving@192.168.2.212. No credentials here — the corpus account's
            # existing authorized SSH key carries the auth.
            "serving": ServingResource(
                host=os.environ.get("SERVING_HOST", "192.168.2.212"),
                user=os.environ.get("SERVING_USER", "serving"),
            ),
        },
    )
