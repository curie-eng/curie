"""Schema bootstrap SQL shared by online and offline migrations."""


def _literal(value: str) -> str:
    return "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def create_schema_if_absent(schema: str) -> str:
    name = _literal(schema)
    identifier = '"' + schema.replace('"', '""') + '"'
    body = f"""BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = {name}) THEN
        CREATE SCHEMA {identifier};
    END IF;
END"""
    return "DO " + _literal(body)
