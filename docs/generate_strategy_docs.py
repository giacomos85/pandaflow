import os

from jinja2 import Environment, FileSystemLoader
from pandaflow.core.registry import load_strategy_classes

DOCS_DIR = "source/strategies"
os.makedirs(DOCS_DIR, exist_ok=True)

env = Environment(loader=FileSystemLoader("source/_templates"))
template = env.get_template("strategy.rst.j2")

strategies = load_strategy_classes()
for name, strategy_versions in strategies.items():
    latest_version = sorted(strategy_versions.keys(), reverse=True)[0]
    strategy_cls = strategy_versions[latest_version]

    meta = getattr(strategy_cls, "meta", {})
    model = getattr(strategy_cls, "strategy_model", None)

    fields = []
    if model:
        for fname, info in model.model_fields.items():
            annotation = info.annotation.__name__ if hasattr(info.annotation, "__name__") else str(info.annotation)
            fields.append({
                "name": fname,
                "type": annotation,
                "description": info.description or "No description",
                "required": info.is_required()
            })
    rendered = template.render(
        name=name,
        version=meta.get("version", "unknown"),
        author=meta.get("author", "unknown"),
        description=meta.get("description", ""),
        fields=fields
    )

    with open(f"{DOCS_DIR}/{name}.rst", "w") as f:
        f.write(rendered)
