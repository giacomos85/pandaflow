import csv
from watchdog.events import FileSystemEventHandler
from pathlib import Path

from pandaflow.core.log import logger
from pandaflow.core.transformer import transform_dataframe

from pandaflow.core.reader import read_csv


class CsvEventHandler(FileSystemEventHandler):
    def __init__(self, config, output_dir, verbose):
        self.config = config
        self.output_dir = Path(output_dir)
        self.verbose = verbose

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        input_path = Path(event.src_path)
        input_file = read_csv(input, self.config)

        output_path = self.output_dir / input_path.name
        logger.info(f"📄  Detected new file: {input_path}")
        try:
            df = transform_dataframe(input_file, self.config)
            df.to_csv(
                output_path, sep=",", index=False, quoting=csv.QUOTE_ALL, quotechar='"'
            )
            logger.info(f"✅  Processed and saved to: {output_path}")
        except Exception as e:
            logger.error(f"Error: {str(e)}")

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        input_path = Path(event.src_path)
        output_path = self.output_dir / input_path.name
        logger.info(f"✏️  Detected modified file: {input_path}")
        try:
            input_file = read_csv(input_path, self.config)
            df = transform_dataframe(input_file, self.config)
            df.to_csv(
                output_path, sep=",", index=False, quoting=csv.QUOTE_ALL, quotechar='"'
            )
            logger.info(f"🔄  Reprocessed and saved to: {output_path}")
        except Exception as e:
            logger.error(f"{str(e)} [file: {input_path}]")
