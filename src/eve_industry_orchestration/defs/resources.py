"""Resource registration for the orchestration project."""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "corpus": CorpusResource(
                binary_path=dg.EnvVar("CORPUS_BINARY_PATH"),
                datasets_dir=dg.EnvVar("CORPUS_DATASETS_DIR"),
                sink_path=dg.EnvVar("CORPUS_SINK_PATH"),
            ),
        },
    )
