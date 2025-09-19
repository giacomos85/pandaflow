# 📦 PandaFlow

![PyPI](https://img.shields.io/pypi/v/pandaflow) ![License](https://img.shields.io/pypi/l/license/pandaflow) ![Coverage](https://codecov.io/gh/user/repo/branch/main/graph/badge.svg) [![CI](https://github.com/giacomos85/pandaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/giacomos85/pandaflow/actions/workflows/ci.yml)

<img src="doc/source/_static/logo.png" alt="Project Logo" width="400">

**PandaFlow** is a modular, strategy-driven CLI tool for transforming CSV files using declarative configuration. Built for maintainability, extensibility, and safety, it empowers users to define transformation pipelines with schema-aware validation and plugin-based logic.

---

## 🚀 Features

- ✅ **Strategy Pattern** — Each transformation is encapsulated in a reusable strategy  
- 🧩 **Config-Driven Execution** — Define pipelines in YAML, TOML, or JSON  
- 🔍 **Schema Validation** — Pydantic-powered rule parsing with clear error reporting  
- 🛠 **CLI Interface** — Run transformations with intuitive commands and feedback  
- 🔌 **Plugin Architecture** — Easily extend with custom strategies  
- 🧪 **Test Coverage** — Comprehensive suite with CI-ready structure  

---

## ⚙️ Usage

### CLI

```bash
pandaflow run --config config.json --input data.csv --output transformed.csv
```

## Config Example

```json
{
  "meta": {
    "csv_separator": ","
  },
  "match": {
    "filename": null,
    "glob": "**/bgsaxo*.csv",
    "regex": null
  },
  "rules": [
    {
      "field": "__md5__",
      "strategy": "hash",
      "source": [
        "ID Cliente",
        "Data della negoziazione",
        "Data valuta",
        "Tipo",
        "Nome prodotto",
        "Instrument ISIN",
        "Valuta strumento",
        "Mercato",
        "Simbolo strumento",
        "_Tipo",
        "Importo contabilizzato",
        "ID ordine",
        "Tasso di conversione"
      ],
      "function": "calculate_md5"
    },
    {
      "field": "__source__",
      "strategy": "constant",
      "value": "bgsaxo"
    }
  ]
}
```

## 🧰 Development Setup

Install Poetry
```bash
curl -sSL https://install.python-poetry.org | python3 -`
```
Install Dependencies

```bash
poetry install
```