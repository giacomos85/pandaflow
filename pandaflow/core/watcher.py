import csv
from watchdog.events import FileSystemEventHandler
from pathlib import Path

from pandaflow.core.log import logger
from pandaflow.core.transformer import transform_dataframe
from pandaflow.core.extract import read_csv


class CsvEventHandler(FileSystemEventHandler):
    def __init__(
        self, config, output_dir, verbose=False, output_format="csv", target_file=None
    ):
        self.config = config
        self.output_dir = Path(output_dir)
        self.verbose = verbose
        self.output_format = output_format
        self.target_file = target_file  # Optional: restrict to a single file

    def _should_process(self, path: Path) -> bool:
        if path.suffix != ".csv":
            return False
        if self.target_file and path.name != self.target_file:
            return False
        return True

    def _process(self, input_path: Path, event_type: str):
        if not self._should_process(input_path):
            return

        logger.info(f"{event_type} detected: {input_path}")
        try:
            df = transform_dataframe(read_csv(input_path, self.config), self.config)
            output_path = (
                self.output_dir / input_path.with_suffix(f".{self.output_format}").name
            )

            if self.output_format == "csv":
                df.to_csv(
                    output_path,
                    sep=",",
                    index=False,
                    quoting=csv.QUOTE_ALL,
                    quotechar='"',
                )
            elif self.output_format == "json":
                df.to_json(output_path, orient="records", lines=True)

            logger.info(f"✅ Saved to: {output_path}")
        except Exception as e:
            logger.error(f"❌ Error processing {input_path}: {e}")

    def on_created(self, event):
        if event.is_directory:
            return
        self._process(Path(event.src_path), "📄 New file")

    def on_modified(self, event):
        if event.is_directory:
            return
        self._process(Path(event.src_path), "✏️ Modified file")
