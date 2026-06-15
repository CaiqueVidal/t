import json
from pathlib import Path
from typing import Any

from app.config.logger import get_logger
from app.schema.analyze import validate_records_structure

logger = get_logger(__name__)


ALLOWED_DATASET_BASE_DIR = Path("datasets").resolve()


def load_records_from_json_file(path_dataset: str) -> list[dict[str, Any]]:
    logger.info("Carregando dataset")
    path = resolve_dataset_path(path_dataset)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path_dataset}")

    if not path.is_file():
        raise ValueError(f"O path informado não é um arquivo: {path_dataset}")

    if path.suffix.lower() != ".json":
        raise ValueError("Apenas arquivos .json são suportados")

    with path.open("r", encoding="utf-8") as file:
        content = json.load(file)

    records = extract_records_from_json_content(content)
    validate_records_structure(records)
    return records


def resolve_dataset_path(path_dataset: str) -> Path:
    path = Path(path_dataset)

    if not path.is_absolute():
        path = Path.cwd() / path

    resolved_path = path.resolve()

    if not str(resolved_path).startswith(str(ALLOWED_DATASET_BASE_DIR)):
        raise ValueError(
            f"path_dataset deve estar dentro do diretório permitido: {ALLOWED_DATASET_BASE_DIR}"
        )

    return resolved_path


def extract_records_from_json_content(content: Any) -> list[dict[str, Any]]:
    """
    Suporta dois formatos:

    1. Arquivo JSON direto como lista:
       [
         {"id": 1, "status": "critical"},
         {"id": 2, "status": "high"}
       ]

    2. Arquivo JSON com chave records:
       {
         "records": [
           {"id": 1, "status": "critical"}
         ]
       }
    """

    if isinstance(content, list):
        records = content

    elif isinstance(content, dict) and isinstance(content.get("records"), list):
        records = content["records"]

    else:
        raise ValueError(
            "JSON inválido. O arquivo deve conter uma lista de registros ou um objeto com a chave 'records'."
        )

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"record[{index}] no arquivo precisa ser um objeto JSON")

    return records
