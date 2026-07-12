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
                # Optional: only `corpus enrich embed` (news-embeddings) needs the
                # local ONNX artifact, so an unset var must not break every other
                # dataset's resource init — hence os.environ.get, not dg.EnvVar.
                embedding_model_dir=os.environ.get("CORPUS_EMBEDDING_MODEL_DIR", ""),
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
