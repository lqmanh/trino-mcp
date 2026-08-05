import json
from pathlib import Path

from trino_mcp import manifest

OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "schemas"
    / f"manifest.v{manifest.SCHEMA_VERSION}.json"
)


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(manifest.json_schema(), indent=2) + "\n")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
