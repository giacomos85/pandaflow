from pathlib import Path
import time
from typing import Dict, Mapping
from pandaflow.models.config import PandaFlowConfig, TransformationNode
import pandas as pd
from pandaflow.core.log import logger


def transform_dataframe(
    df: pd.DataFrame, config: PandaFlowConfig, output_path: Path = None
) -> pd.DataFrame | None:
    """Transform CSV input based on config transformations.

    Args:
        input_source: Pandas dataframe.
        config: Dictionary containing meta, match, and transformations.

    Returns:
        Transformed DataFrame, or None if skipped due to match transformations.
    """
    dag = []
    for i, tr in enumerate(config.transformations):
        strategy_name = tr.meta.get("name")
        start = time.time()
        df = tr.run(df)
        if config._profile:
            duration = time.time() - start
            logger.info(f"⏱️ step_{i}_{strategy_name}: {duration:.3f}s")
        dag.append(
            TransformationNode(
                name=f"step_{i}_{strategy_name}",
                strategy=strategy_name,
                depends_on=[dag[-1].name] if i > 0 else [],
                output_preview=str(df.shape),
            )
        )
    return df


def transform(
    input_mapping: Mapping[Path, pd.DataFrame | None],
    config: dict,
    output_path: Path = None,
) -> Dict[Path, pd.DataFrame | None]:
    """Pure batch transformation of multiple CSV files.

    Args:
        input_mapping: Dict mapping each input file to its DataFrame.
        config: Dictionary of transformation transformations.

    Returns:
        Dict mapping each input file to its transformed DataFrame,
        or None if the file was skipped due to match transformations.
    """
    results = {}

    for input_file, df in input_mapping.items():
        df = transform_dataframe(df, config, output_path=output_path)
        results[input_file] = df  # may be None if skipped

    return results
