const PG_BOOL_OID: u32 = 16;
const PG_INT8_OID: u32 = 20;
const PG_TEXT_OID: u32 = 25;

pub(crate) struct VirtualRelationColumnDef {
    pub(crate) name: String,
    pub(crate) type_oid: u32,
}

pub(crate) fn lookup_virtual_relation(
    name: &[String],
) -> Option<(String, String, Vec<VirtualRelationColumnDef>)> {
    let (schema, relation) = resolve_virtual_relation_name(name)?;
    let columns = virtual_relation_column_defs(&schema, &relation)?;
    Some((schema, relation, columns))
}

fn resolve_virtual_relation_name(name: &[String]) -> Option<(String, String)> {
    let normalized = name
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>();
    match normalized.as_slice() {
        [relation] if is_pg_catalog_virtual_relation(relation) => {
            Some(("pg_catalog".to_string(), relation.to_string()))
        }
        [schema, relation]
            if schema == "pg_catalog" && is_pg_catalog_virtual_relation(relation) =>
        {
            Some((schema.to_string(), relation.to_string()))
        }
        [schema, relation]
            if schema == "information_schema"
                && is_information_schema_virtual_relation(relation) =>
        {
            Some((schema.to_string(), relation.to_string()))
        }
        [schema, relation] if schema == "ws" && relation == "connections" => {
            Some(("ws".to_string(), "connections".to_string()))
        }
        _ => None,
    }
}

fn is_pg_catalog_virtual_relation(relation: &str) -> bool {
    matches!(
        relation,
        "pg_namespace" | "pg_class" | "pg_attribute" | "pg_type"
            | "pg_database" | "pg_roles" | "pg_settings"
            | "pg_tables" | "pg_views" | "pg_indexes"
            | "pg_proc" | "pg_constraint" | "pg_extension"
    )
}

fn is_information_schema_virtual_relation(relation: &str) -> bool {
    matches!(relation, "tables" | "columns" | "schemata" | "key_column_usage" | "table_constraints")
}

fn virtual_relation_column_defs(
    schema: &str,
    relation: &str,
) -> Option<Vec<VirtualRelationColumnDef>> {
    let cols = match (schema, relation) {
        ("pg_catalog", "pg_namespace") => vec![
            VirtualRelationColumnDef {
                name: "oid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "nspname".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("pg_catalog", "pg_class") => vec![
            VirtualRelationColumnDef {
                name: "oid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "relname".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "relnamespace".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "relkind".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("pg_catalog", "pg_attribute") => vec![
            VirtualRelationColumnDef {
                name: "attrelid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "attname".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "atttypid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "attnum".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "attnotnull".to_string(),
                type_oid: PG_BOOL_OID,
            },
        ],
        ("pg_catalog", "pg_type") => vec![
            VirtualRelationColumnDef {
                name: "oid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "typname".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("information_schema", "tables") => vec![
            VirtualRelationColumnDef {
                name: "table_schema".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "table_name".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "table_type".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("information_schema", "columns") => vec![
            VirtualRelationColumnDef {
                name: "table_schema".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "table_name".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "column_name".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "ordinal_position".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "data_type".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "is_nullable".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("information_schema", "schemata") => vec![
            VirtualRelationColumnDef { name: "catalog_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "schema_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "schema_owner".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("information_schema", "key_column_usage") => vec![
            VirtualRelationColumnDef { name: "constraint_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_schema".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "column_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "ordinal_position".to_string(), type_oid: PG_INT8_OID },
        ],
        ("information_schema", "table_constraints") => vec![
            VirtualRelationColumnDef { name: "constraint_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_schema".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "constraint_type".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_database") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "datname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "datdba".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "encoding".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "datcollate".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_roles") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "rolname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "rolsuper".to_string(), type_oid: PG_BOOL_OID },
            VirtualRelationColumnDef { name: "rolcanlogin".to_string(), type_oid: PG_BOOL_OID },
        ],
        ("pg_catalog", "pg_settings") => vec![
            VirtualRelationColumnDef { name: "name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "setting".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "category".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "short_desc".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_tables") => vec![
            VirtualRelationColumnDef { name: "schemaname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "tablename".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "tableowner".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_views") => vec![
            VirtualRelationColumnDef { name: "schemaname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "viewname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "viewowner".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_indexes") => vec![
            VirtualRelationColumnDef { name: "schemaname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "tablename".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "indexname".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_proc") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "proname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "pronamespace".to_string(), type_oid: PG_INT8_OID },
        ],
        ("pg_catalog", "pg_constraint") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "conname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "connamespace".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "contype".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "conrelid".to_string(), type_oid: PG_INT8_OID },
        ],
        ("pg_catalog", "pg_extension") => vec![
            VirtualRelationColumnDef { name: "extname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "extversion".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "extdescription".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("ws", "connections") => vec![
            VirtualRelationColumnDef { name: "id".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "url".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "state".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "opened_at".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "messages_in".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "messages_out".to_string(), type_oid: PG_INT8_OID },
        ],
        _ => return None,
    };
    Some(cols)
}

