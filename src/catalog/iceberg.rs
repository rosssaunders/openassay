#![cfg(not(target_arch = "wasm32"))]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    DynObjectStore, ObjectMeta, ObjectStore, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field as ParquetField;
use serde_json::Value as JsonValue;

use super::{CatalogError, Column, Schema, Table, TableKind, TypeSignature};
use crate::parser::ast::{BinaryOp, Expr, UnaryOp};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergStorageKind {
    Local,
    S3,
    Gcs,
    AzureBlob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergObjectLocation {
    raw: String,
    store_kind: IcebergStorageKind,
    bucket: Option<String>,
    path: String,
}

impl IcebergObjectLocation {
    pub fn parse(input: &str) -> Result<Self, CatalogError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(catalog_error("Iceberg path cannot be empty"));
        }

        if let Some((scheme, rest)) = trimmed.split_once("://") {
            let store_kind = match scheme.to_ascii_lowercase().as_str() {
                "s3" => IcebergStorageKind::S3,
                "gs" | "gcs" => IcebergStorageKind::Gcs,
                "az" | "azure" | "abfs" | "abfss" => IcebergStorageKind::AzureBlob,
                "file" => IcebergStorageKind::Local,
                _ => {
                    return Err(catalog_error(format!(
                        "unsupported Iceberg storage URI scheme \"{scheme}\""
                    )));
                }
            };

            if store_kind == IcebergStorageKind::Local {
                let normalized = rest.trim_start_matches('/');
                let local_path = format!("/{normalized}");
                return Ok(Self {
                    raw: trimmed.to_string(),
                    store_kind,
                    bucket: None,
                    path: normalize_object_path(&local_path),
                });
            }

            let (bucket, suffix) = rest
                .split_once('/')
                .map_or((rest, ""), |(bucket, suffix)| (bucket, suffix));
            if bucket.is_empty() {
                return Err(catalog_error(format!(
                    "storage URI \"{trimmed}\" is missing a bucket or container"
                )));
            }
            return Ok(Self {
                raw: trimmed.to_string(),
                store_kind,
                bucket: Some(bucket.to_string()),
                path: normalize_object_path(suffix),
            });
        }

        Ok(Self {
            raw: trimmed.to_string(),
            store_kind: IcebergStorageKind::Local,
            bucket: None,
            path: normalize_object_path(trimmed),
        })
    }

    pub fn store_kind(&self) -> IcebergStorageKind {
        self.store_kind
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn file_name(&self) -> Option<&str> {
        self.path
            .trim_end_matches('/')
            .rsplit('/')
            .find(|part| !part.is_empty())
    }

    pub fn extension(&self) -> Option<&str> {
        self.file_name()
            .and_then(|name| name.rsplit_once('.').map(|(_, ext)| ext))
    }

    pub fn is_directory_hint(&self) -> bool {
        self.raw.ends_with('/') || self.path.is_empty()
    }

    pub fn join(&self, segment: &str) -> Self {
        let base = self.path.trim_end_matches('/');
        let suffix = segment.trim_start_matches('/');
        let path = if base.is_empty() {
            suffix.to_string()
        } else if suffix.is_empty() {
            base.to_string()
        } else {
            format!("{base}/{suffix}")
        };
        Self {
            raw: render_location(self.store_kind, self.bucket(), &path),
            store_kind: self.store_kind,
            bucket: self.bucket.clone(),
            path,
        }
    }

    pub fn parent(&self) -> Option<Self> {
        if self.path.is_empty() {
            return None;
        }
        let trimmed = self.path.trim_end_matches('/');
        let parent = trimmed.rsplit_once('/').map_or("", |(parent, _)| parent);
        Some(Self {
            raw: render_location(self.store_kind, self.bucket(), parent),
            store_kind: self.store_kind,
            bucket: self.bucket.clone(),
            path: parent.to_string(),
        })
    }

    pub fn relative_to(&self, root: &Self) -> Option<String> {
        if self.store_kind != root.store_kind || self.bucket != root.bucket {
            return None;
        }
        let root_path = root.path.trim_matches('/');
        let path = self.path.trim_matches('/');
        if root_path.is_empty() {
            return Some(path.to_string());
        }
        path.strip_prefix(root_path)
            .map(|value| value.trim_start_matches('/').to_string())
    }
}

#[derive(Debug, Clone)]
pub struct IcebergBrowseCatalog {
    pub name: String,
    pub location: String,
    pub namespaces: Vec<IcebergBrowseNamespace>,
}

#[derive(Debug, Clone)]
pub struct IcebergBrowseNamespace {
    pub schema: Schema,
    pub namespace_path: Vec<String>,
    pub tables: Vec<IcebergBrowseTable>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IcebergBrowseTable {
    pub table: Table,
    pub location: String,
    pub metadata: IcebergTableMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IcebergTableMetadata {
    pub table_root: String,
    pub metadata_file: String,
    pub table_uuid: Option<String>,
    pub format_version: Option<i64>,
    pub last_updated_ms: Option<i64>,
    pub current_schema_id: Option<i64>,
    pub partition_spec_json: String,
    pub snapshot_count: i64,
    pub total_data_files: i64,
    pub partition_columns: Vec<String>,
    pub column_aliases: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergSchemaField {
    pub id: i64,
    pub name: String,
    pub type_signature: TypeSignature,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct IcebergResolvedTable {
    table_root: IcebergObjectLocation,
    current_schema: Vec<IcebergSchemaField>,
    alias_map: HashMap<String, String>,
    partition_columns: Vec<String>,
    metadata: IcebergTableMetadata,
    parquet_files: Vec<IcebergDiscoveredFile>,
}

#[derive(Debug, Clone)]
pub struct IcebergScanPlan {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
    pub scanned_files: usize,
    pub pruned_files: usize,
    pub metadata: IcebergTableMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IcebergFileScanSummary {
    pub file_path: String,
    pub partition_values: HashMap<String, ScalarValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IcebergPartitionPredicate {
    Eq(String, ScalarValue),
    Lt(String, ScalarValue),
    Lte(String, ScalarValue),
    Gt(String, ScalarValue),
    Gte(String, ScalarValue),
    In(String, Vec<ScalarValue>),
    Between(String, ScalarValue, ScalarValue),
}

#[derive(Debug, Clone)]
struct IcebergDiscoveredFile {
    location: IcebergObjectLocation,
    partition_values: HashMap<String, ScalarValue>,
}

#[derive(Debug, Clone)]
struct IcebergStorage {
    kind: IcebergStorageKind,
    bucket: Option<String>,
    store: Arc<DynObjectStore>,
}

impl IcebergStorage {
    async fn from_location(location: &IcebergObjectLocation) -> Result<Self, CatalogError> {
        let store: Arc<DynObjectStore> = match location.store_kind() {
            IcebergStorageKind::Local => Arc::new(LocalFileSystem::new()),
            IcebergStorageKind::S3 => Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(location.bucket().unwrap_or_default())
                    .build()
                    .map_err(to_catalog_error)?,
            ),
            IcebergStorageKind::Gcs => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(location.bucket().unwrap_or_default())
                    .build()
                    .map_err(to_catalog_error)?,
            ),
            IcebergStorageKind::AzureBlob => Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_container_name(location.bucket().unwrap_or_default())
                    .build()
                    .map_err(to_catalog_error)?,
            ),
        };

        Ok(Self {
            kind: location.store_kind(),
            bucket: location.bucket().map(ToOwned::to_owned),
            store,
        })
    }

    async fn read_to_string(
        &self,
        location: &IcebergObjectLocation,
    ) -> Result<String, CatalogError> {
        if self.kind == IcebergStorageKind::Local {
            std::fs::read_to_string(location.path()).map_err(to_catalog_error)
        } else {
            let bytes = self
                .store
                .get(&object_path(location.path()))
                .await
                .map_err(to_catalog_error)?
                .bytes()
                .await
                .map_err(to_catalog_error)?;
            String::from_utf8(bytes.to_vec()).map_err(to_catalog_error)
        }
    }

    async fn read_bytes(&self, location: &IcebergObjectLocation) -> Result<Vec<u8>, CatalogError> {
        match self.kind {
            IcebergStorageKind::Local => std::fs::read(location.path()).map_err(to_catalog_error),
            _ => Ok(self
                .store
                .get(&object_path(location.path()))
                .await
                .map_err(to_catalog_error)?
                .bytes()
                .await
                .map_err(to_catalog_error)?
                .to_vec()),
        }
    }

    async fn list_recursive(
        &self,
        prefix: &IcebergObjectLocation,
    ) -> Result<Vec<IcebergObjectLocation>, CatalogError> {
        if self.kind == IcebergStorageKind::Local {
            let mut out = Vec::new();
            collect_local_files(Path::new(prefix.path()), &mut out)?;
            return Ok(out
                .into_iter()
                .map(|path| IcebergObjectLocation {
                    raw: path.clone(),
                    store_kind: IcebergStorageKind::Local,
                    bucket: None,
                    path,
                })
                .collect());
        }

        let entries = self
            .store
            .list(Some(&object_path(prefix.path())))
            .try_collect::<Vec<ObjectMeta>>()
            .await
            .map_err(to_catalog_error)?;
        Ok(entries
            .into_iter()
            .map(|entry| IcebergObjectLocation {
                raw: render_location(self.kind, self.bucket.as_deref(), entry.location.as_ref()),
                store_kind: self.kind,
                bucket: self.bucket.clone(),
                path: entry.location.to_string(),
            })
            .collect())
    }
}

pub async fn browse_iceberg_catalogs(
    root: &str,
) -> Result<Vec<IcebergBrowseCatalog>, CatalogError> {
    let location = IcebergObjectLocation::parse(root)?;
    let table_roots = discover_table_roots(&location).await?;
    if table_roots.is_empty() {
        return Err(catalog_error(format!(
            "no Iceberg tables found under {}",
            location.raw()
        )));
    }

    let layout = IcebergCatalogLayout::from_table_roots(&location, &table_roots);
    let mut catalogs = BTreeMap::<String, BTreeMap<String, Vec<IcebergBrowseTable>>>::new();
    let mut namespace_paths =
        HashMap::<(String, String), Vec<String>>::with_capacity(table_roots.len());

    let mut next_schema_oid = 20_000_u32;
    let mut next_table_oid = 30_000_u32;
    let mut next_column_oid = 40_000_u32;

    for root_location in table_roots {
        let resolved = resolve_iceberg_table(root_location.raw()).await?;
        let entry = layout.entry_for(&location, &resolved.table_root);
        let schema_name = if entry.namespace_path.is_empty() {
            "default".to_string()
        } else {
            entry.namespace_path.join(".")
        };

        let columns = resolved
            .current_schema
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let oid = next_column_oid;
                next_column_oid += 1;
                Column::new(
                    oid,
                    field.name.clone(),
                    field.type_signature,
                    crate::parser::ast::SubscriptValueType::Other,
                    idx as u16,
                    field.nullable,
                    false,
                    false,
                    None,
                    None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let table = Table::new(
            next_table_oid,
            next_schema_oid,
            schema_name.clone(),
            entry.table_name.clone(),
            TableKind::Heap,
            columns,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );
        next_table_oid += 1;

        let browse_table = IcebergBrowseTable {
            table,
            location: resolved.table_root.raw().to_string(),
            metadata: resolved.metadata.clone(),
        };

        catalogs
            .entry(entry.catalog_name.clone())
            .or_default()
            .entry(schema_name.clone())
            .or_default()
            .push(browse_table);
        namespace_paths.insert(
            (entry.catalog_name.clone(), schema_name),
            entry.namespace_path.clone(),
        );
    }

    let mut out = Vec::new();
    for (catalog_name, schemas) in catalogs {
        let location = layout.catalog_location(&location, &catalog_name);
        let namespaces = schemas
            .into_iter()
            .map(|(schema_name, mut tables)| {
                tables.sort_by(|left, right| left.table.name().cmp(right.table.name()));
                let schema = Schema::new(next_schema_oid, schema_name.clone());
                next_schema_oid += 1;
                IcebergBrowseNamespace {
                    schema,
                    namespace_path: namespace_paths
                        .remove(&(catalog_name.clone(), schema_name))
                        .unwrap_or_default(),
                    tables,
                }
            })
            .collect::<Vec<_>>();
        out.push(IcebergBrowseCatalog {
            name: catalog_name,
            location,
            namespaces,
        });
    }

    out.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(out)
}

pub async fn resolve_iceberg_table(path: &str) -> Result<IcebergResolvedTable, CatalogError> {
    let input = IcebergObjectLocation::parse(path)?;
    let table_root = resolve_iceberg_table_root(&input)?;
    let storage = IcebergStorage::from_location(&table_root).await?;
    let metadata_file = resolve_iceberg_metadata_file(&storage, &input).await?;
    let metadata_text = storage.read_to_string(&metadata_file).await?;
    let metadata_json =
        serde_json::from_str::<JsonValue>(&metadata_text).map_err(to_catalog_error)?;

    let current_schema_id = metadata_json
        .get("current-schema-id")
        .and_then(JsonValue::as_i64);
    let schemas = metadata_json
        .get("schemas")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| catalog_error("Iceberg metadata is missing schemas"))?;
    let current_schema_json = if let Some(current_schema_id) = current_schema_id {
        schemas
            .iter()
            .find(|schema| {
                schema.get("schema-id").and_then(JsonValue::as_i64) == Some(current_schema_id)
            })
            .or_else(|| schemas.last())
    } else {
        schemas.last()
    }
    .ok_or_else(|| catalog_error("Iceberg metadata did not contain a usable schema"))?;

    let current_schema = parse_schema_fields(current_schema_json)?;
    let alias_map = collect_schema_aliases(schemas, &current_schema);
    let partition_columns = current_partition_columns(&metadata_json);
    let parquet_files =
        discover_iceberg_parquet_files(&storage, &table_root, &partition_columns, &current_schema)
            .await?;

    let metadata = IcebergTableMetadata {
        table_root: table_root.raw().to_string(),
        metadata_file: metadata_file.raw().to_string(),
        table_uuid: metadata_json
            .get("table-uuid")
            .and_then(JsonValue::as_str)
            .map(ToOwned::to_owned),
        format_version: metadata_json
            .get("format-version")
            .and_then(JsonValue::as_i64),
        last_updated_ms: metadata_json
            .get("last-updated-ms")
            .and_then(JsonValue::as_i64),
        current_schema_id,
        partition_spec_json: metadata_json
            .get("partition-specs")
            .cloned()
            .unwrap_or_else(|| JsonValue::Array(Vec::new()))
            .to_string(),
        snapshot_count: metadata_json
            .get("snapshots")
            .and_then(JsonValue::as_array)
            .map_or(0_i64, |items| items.len() as i64),
        total_data_files: parquet_files.len() as i64,
        partition_columns,
        column_aliases: alias_map.clone(),
    };

    Ok(IcebergResolvedTable {
        table_root,
        current_schema,
        alias_map,
        partition_columns: metadata.partition_columns.clone(),
        metadata,
        parquet_files,
    })
}

pub async fn read_iceberg_metadata(path: &str) -> Result<IcebergTableMetadata, CatalogError> {
    Ok(resolve_iceberg_table(path).await?.metadata)
}

pub async fn plan_iceberg_scan(
    path: &str,
    requested_columns: Option<&[String]>,
    partition_predicates: &[IcebergPartitionPredicate],
) -> Result<IcebergScanPlan, CatalogError> {
    let resolved = resolve_iceberg_table(path).await?;
    let requested = requested_columns.map(|cols| {
        cols.iter()
            .map(|column| column.to_ascii_lowercase())
            .collect::<HashSet<_>>()
    });
    let output_fields = resolved
        .current_schema
        .iter()
        .filter(|field| {
            requested
                .as_ref()
                .is_none_or(|requested| requested.contains(&field.name.to_ascii_lowercase()))
        })
        .cloned()
        .collect::<Vec<_>>();
    let output_columns = output_fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let output_index = output_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name.to_ascii_lowercase(), idx))
        .collect::<HashMap<_, _>>();

    let files = if partition_predicates.is_empty() {
        resolved.parquet_files.clone()
    } else {
        resolved
            .parquet_files
            .iter()
            .filter(|file| file_matches_partition_predicates(file, partition_predicates))
            .cloned()
            .collect::<Vec<_>>()
    };

    let scanned_files = files.len();
    let pruned_files = resolved.parquet_files.len().saturating_sub(scanned_files);
    let storage = IcebergStorage::from_location(&resolved.table_root).await?;
    let mut rows = Vec::new();

    for file in files {
        let file_rows = scan_single_parquet_file(
            &storage,
            &file.location,
            &output_fields,
            &output_index,
            &resolved.alias_map,
        )
        .await?;
        rows.extend(file_rows);
    }

    Ok(IcebergScanPlan {
        columns: output_columns,
        rows,
        scanned_files,
        pruned_files,
        metadata: resolved.metadata,
    })
}

pub async fn scan_iceberg_table_with_predicate(
    path: &str,
    predicate: Option<&Expr>,
    qualifiers: &[String],
    params: &[Option<ScalarValue>],
) -> Result<IcebergScanPlan, EngineError> {
    let resolved = resolve_iceberg_table(path).await.map_err(to_engine_error)?;
    let partition_fields = resolved
        .current_schema
        .iter()
        .filter(|field| {
            resolved
                .partition_columns
                .iter()
                .any(|partition| partition.eq_ignore_ascii_case(&field.name))
        })
        .cloned()
        .collect::<Vec<_>>();
    let partition_predicates = predicate
        .map(|predicate| {
            extract_partition_predicates(predicate, qualifiers, &partition_fields, params)
        })
        .transpose()?
        .unwrap_or_default();
    plan_iceberg_scan(path, None, &partition_predicates)
        .await
        .map_err(to_engine_error)
}

pub async fn list_iceberg_data_files(
    path: &str,
    partition_predicates: &[IcebergPartitionPredicate],
) -> Result<Vec<IcebergFileScanSummary>, CatalogError> {
    let resolved = resolve_iceberg_table(path).await?;
    Ok(resolved
        .parquet_files
        .into_iter()
        .filter(|file| file_matches_partition_predicates(file, partition_predicates))
        .map(|file| IcebergFileScanSummary {
            file_path: file.location.raw().to_string(),
            partition_values: file.partition_values,
        })
        .collect())
}

pub fn extract_partition_predicates(
    predicate: &Expr,
    qualifiers: &[String],
    partition_fields: &[IcebergSchemaField],
    params: &[Option<ScalarValue>],
) -> Result<Vec<IcebergPartitionPredicate>, EngineError> {
    let partition_lookup = partition_fields
        .iter()
        .map(|field| (field.name.to_ascii_lowercase(), field.clone()))
        .collect::<HashMap<_, _>>();
    let mut out = Vec::new();
    for conjunct in decompose_and_conjuncts(predicate) {
        if let Some(extracted) =
            extract_single_partition_predicate(&conjunct, qualifiers, &partition_lookup, params)?
        {
            out.push(extracted);
        }
    }
    Ok(out)
}

pub fn compare_iceberg_metadata_files(left: &str, right: &str) -> std::cmp::Ordering {
    iceberg_metadata_version(left)
        .cmp(&iceberg_metadata_version(right))
        .then_with(|| left.cmp(right))
}

pub fn iceberg_metadata_version(file_name: &str) -> u64 {
    let stem = file_name
        .strip_suffix(".metadata.json")
        .unwrap_or(file_name);
    let stem = stem.strip_prefix('v').unwrap_or(stem);
    let digits = stem
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse::<u64>().unwrap_or(0)
}

fn current_partition_columns(metadata_json: &JsonValue) -> Vec<String> {
    metadata_json
        .get("partition-specs")
        .and_then(JsonValue::as_array)
        .and_then(|specs| specs.last())
        .and_then(|spec| spec.get("fields"))
        .and_then(JsonValue::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| field.get("name").and_then(JsonValue::as_str))
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_schema_fields(schema: &JsonValue) -> Result<Vec<IcebergSchemaField>, CatalogError> {
    let fields = schema
        .get("fields")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| catalog_error("Iceberg schema is missing fields"))?;
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        let id = field
            .get("id")
            .and_then(JsonValue::as_i64)
            .ok_or_else(|| catalog_error("Iceberg schema field is missing an id"))?;
        let name = field
            .get("name")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| catalog_error("Iceberg schema field is missing a name"))?
            .to_string();
        let required = field
            .get("required")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let type_signature = parse_type_signature(field.get("type")).unwrap_or(TypeSignature::Text);
        out.push(IcebergSchemaField {
            id,
            name,
            type_signature,
            nullable: !required,
        });
    }
    Ok(out)
}

fn parse_type_signature(value: Option<&JsonValue>) -> Option<TypeSignature> {
    let type_name = match value {
        Some(JsonValue::String(value)) => value.as_str(),
        Some(JsonValue::Object(map)) => map.get("type")?.as_str()?,
        _ => return None,
    };
    Some(match type_name {
        "boolean" => TypeSignature::Bool,
        "int" | "long" => TypeSignature::Int8,
        "float" | "double" => TypeSignature::Float8,
        "decimal" => TypeSignature::Numeric,
        "date" => TypeSignature::Date,
        "timestamp" | "timestamptz" => TypeSignature::Timestamp,
        _ => TypeSignature::Text,
    })
}

fn collect_schema_aliases(
    schemas: &[JsonValue],
    current_schema: &[IcebergSchemaField],
) -> HashMap<String, String> {
    let current_names = current_schema
        .iter()
        .map(|field| (field.id, field.name.clone()))
        .collect::<HashMap<_, _>>();
    let mut aliases = HashMap::new();
    for schema in schemas {
        let Ok(fields) = parse_schema_fields(schema) else {
            continue;
        };
        for field in fields {
            let Some(current_name) = current_names.get(&field.id) else {
                continue;
            };
            aliases.insert(field.name.to_ascii_lowercase(), current_name.clone());
        }
    }
    aliases
}

async fn discover_table_roots(
    root: &IcebergObjectLocation,
) -> Result<Vec<IcebergObjectLocation>, CatalogError> {
    if can_be_direct_table(root)
        && let Ok(direct) = resolve_iceberg_table_root(root)
    {
        let storage = IcebergStorage::from_location(&direct).await?;
        if resolve_iceberg_metadata_file(&storage, root).await.is_ok() {
            return Ok(vec![direct]);
        }
    }

    let storage = IcebergStorage::from_location(root).await?;
    let objects = storage.list_recursive(root).await?;
    let mut roots: HashSet<String> = HashSet::new();
    for object in objects {
        let Some(file_name) = object.file_name() else {
            continue;
        };
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(parent) = object.parent() else {
            continue;
        };
        let Some(root_dir) = (if parent
            .file_name()
            .is_some_and(|name| name.eq_ignore_ascii_case("metadata"))
        {
            parent.parent()
        } else {
            object.parent()
        }) else {
            continue;
        };
        roots.insert(root_dir.raw().to_string());
    }

    let mut out = roots
        .into_iter()
        .map(|value| IcebergObjectLocation::parse(value.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    out.sort_by(|left, right| left.raw().cmp(right.raw()));
    Ok(out)
}

fn can_be_direct_table(location: &IcebergObjectLocation) -> bool {
    location.store_kind() == IcebergStorageKind::Local || !location.path().is_empty()
}

fn resolve_iceberg_table_root(
    location: &IcebergObjectLocation,
) -> Result<IcebergObjectLocation, CatalogError> {
    if location.is_directory_hint() {
        return Ok(location.clone());
    }
    if location
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
    {
        return location.parent().ok_or_else(|| {
            catalog_error(format!(
                "cannot determine Iceberg table root from {}",
                location.raw()
            ))
        });
    }

    if location
        .file_name()
        .is_some_and(|name| name.ends_with(".metadata.json"))
    {
        let parent = location.parent().ok_or_else(|| {
            catalog_error(format!(
                "cannot determine Iceberg table root from {}",
                location.raw()
            ))
        })?;
        if parent
            .file_name()
            .is_some_and(|name| name.eq_ignore_ascii_case("metadata"))
        {
            return parent.parent().ok_or_else(|| {
                catalog_error(format!(
                    "cannot determine Iceberg table root from {}",
                    location.raw()
                ))
            });
        }
        return Ok(parent);
    }

    Ok(location.clone())
}

async fn resolve_iceberg_metadata_file(
    storage: &IcebergStorage,
    input: &IcebergObjectLocation,
) -> Result<IcebergObjectLocation, CatalogError> {
    if input
        .file_name()
        .is_some_and(|name| name.ends_with(".metadata.json"))
    {
        return Ok(input.clone());
    }

    let table_root = resolve_iceberg_table_root(input)?;
    let metadata_dir = table_root.join("metadata");
    let mut candidates = storage
        .list_recursive(&metadata_dir)
        .await?
        .into_iter()
        .filter(|entry| {
            entry
                .file_name()
                .is_some_and(|name| name.ends_with(".metadata.json"))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        compare_iceberg_metadata_files(
            left.file_name().unwrap_or_default(),
            right.file_name().unwrap_or_default(),
        )
    });
    candidates.pop().ok_or_else(|| {
        catalog_error(format!(
            "no Iceberg metadata found under {}",
            table_root.raw()
        ))
    })
}

async fn discover_iceberg_parquet_files(
    storage: &IcebergStorage,
    table_root: &IcebergObjectLocation,
    partition_columns: &[String],
    current_schema: &[IcebergSchemaField],
) -> Result<Vec<IcebergDiscoveredFile>, CatalogError> {
    let data_dir = table_root.join("data");
    let mut candidates = storage.list_recursive(&data_dir).await?;
    if candidates.is_empty() {
        candidates = storage.list_recursive(table_root).await?;
    }
    let partition_lookup = current_schema
        .iter()
        .map(|field| (field.name.to_ascii_lowercase(), field.type_signature))
        .collect::<HashMap<_, _>>();
    let mut files = Vec::new();
    for entry in candidates {
        if !entry
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
        {
            continue;
        }
        let partition_values =
            parse_partition_values(&entry, &data_dir, partition_columns, &partition_lookup);
        files.push(IcebergDiscoveredFile {
            location: entry,
            partition_values,
        });
    }
    files.sort_by(|left, right| left.location.raw().cmp(right.location.raw()));
    Ok(files)
}

fn parse_partition_values(
    file: &IcebergObjectLocation,
    data_root: &IcebergObjectLocation,
    partition_columns: &[String],
    type_lookup: &HashMap<String, TypeSignature>,
) -> HashMap<String, ScalarValue> {
    let mut out = HashMap::new();
    let Some(relative) = file.relative_to(data_root) else {
        return out;
    };
    for component in relative.split('/') {
        let Some((name, value)) = component.split_once('=') else {
            continue;
        };
        if !partition_columns
            .iter()
            .any(|partition| partition.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let type_signature = type_lookup
            .get(&name.to_ascii_lowercase())
            .copied()
            .unwrap_or(TypeSignature::Text);
        let scalar = coerce_text_to_type(value, type_signature)
            .unwrap_or_else(|_| ScalarValue::Text(value.to_string()));
        out.insert(name.to_ascii_lowercase(), scalar);
    }
    out
}

pub async fn plan_parquet_scan(path: &str) -> Result<IcebergScanPlan, CatalogError> {
    let location = IcebergObjectLocation::parse(path)?;
    let storage = IcebergStorage::from_location(&location).await?;

    let files = discover_raw_parquet_files(&storage, &location).await?;
    if files.is_empty() {
        return Err(catalog_error(format!(
            "no parquet files found at {}",
            location.raw()
        )));
    }

    let schema_fields = {
        let first = &files[0];
        if storage.kind == IcebergStorageKind::Local {
            let file = std::fs::File::open(first.path()).map_err(to_catalog_error)?;
            let reader = SerializedFileReader::new(file).map_err(to_catalog_error)?;
            infer_schema_from_parquet(&reader)?
        } else {
            let bytes = Bytes::from(storage.read_bytes(first).await?);
            let reader = SerializedFileReader::new(bytes).map_err(to_catalog_error)?;
            infer_schema_from_parquet(&reader)?
        }
    };

    let output_columns = schema_fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let output_index = schema_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name.to_ascii_lowercase(), idx))
        .collect::<HashMap<_, _>>();
    let empty_aliases = HashMap::new();

    let scanned_files = files.len();
    let mut rows = Vec::new();
    for file in &files {
        let file_rows = scan_single_parquet_file(
            &storage,
            file,
            &schema_fields,
            &output_index,
            &empty_aliases,
        )
        .await?;
        rows.extend(file_rows);
    }

    Ok(IcebergScanPlan {
        columns: output_columns,
        rows,
        scanned_files,
        pruned_files: 0,
        metadata: IcebergTableMetadata {
            table_root: location.raw().to_string(),
            metadata_file: String::new(),
            table_uuid: None,
            format_version: None,
            last_updated_ms: None,
            current_schema_id: None,
            partition_spec_json: String::new(),
            snapshot_count: 0,
            total_data_files: scanned_files as i64,
            partition_columns: Vec::new(),
            column_aliases: HashMap::new(),
        },
    })
}

async fn discover_raw_parquet_files(
    storage: &IcebergStorage,
    location: &IcebergObjectLocation,
) -> Result<Vec<IcebergObjectLocation>, CatalogError> {
    if location
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
    {
        if storage.kind == IcebergStorageKind::Local && !Path::new(location.path()).exists() {
            return Err(catalog_error(format!(
                "parquet file not found: {}",
                location.raw()
            )));
        }
        return Ok(vec![location.clone()]);
    }

    let mut candidates = storage.list_recursive(location).await?;
    candidates.retain(|entry| {
        entry
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
    });
    candidates.sort_by(|a, b| a.raw().cmp(b.raw()));
    Ok(candidates)
}

fn infer_schema_from_parquet<R>(
    reader: &SerializedFileReader<R>,
) -> Result<Vec<IcebergSchemaField>, CatalogError>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    let schema = reader.metadata().file_metadata().schema_descr();
    let root = schema.root_schema();
    let fields = match root {
        parquet::schema::types::Type::GroupType { fields, .. } => fields,
        _ => {
            return Err(catalog_error("parquet file has no column schema"));
        }
    };

    let mut result = Vec::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let name = field.name().to_string();
        let type_signature = parquet_type_to_type_signature(field);
        let nullable = field.get_basic_info().has_repetition()
            && field.get_basic_info().repetition() != parquet::basic::Repetition::REQUIRED;
        result.push(IcebergSchemaField {
            id: idx as i64,
            name,
            type_signature,
            nullable,
        });
    }
    Ok(result)
}

fn parquet_type_to_type_signature(field: &parquet::schema::types::Type) -> TypeSignature {
    use parquet::basic::ConvertedType;

    if !field.is_primitive() {
        return TypeSignature::Text;
    }

    let physical = field.get_physical_type();
    let converted = field.get_basic_info().converted_type();

    if let Some(logical) = field.get_basic_info().logical_type_ref() {
        use parquet::basic::LogicalType;
        match logical {
            LogicalType::String | LogicalType::Enum | LogicalType::Uuid | LogicalType::Json => {
                return TypeSignature::Text;
            }
            LogicalType::Integer { .. } => return TypeSignature::Int8,
            LogicalType::Decimal { .. } => return TypeSignature::Numeric,
            LogicalType::Date => return TypeSignature::Date,
            LogicalType::Timestamp { .. } => return TypeSignature::Timestamp,
            LogicalType::Float16 => return TypeSignature::Float8,
            _ => {}
        }
    }

    match converted {
        ConvertedType::UTF8 | ConvertedType::ENUM | ConvertedType::JSON => TypeSignature::Text,
        ConvertedType::DATE => TypeSignature::Date,
        ConvertedType::TIMESTAMP_MILLIS | ConvertedType::TIMESTAMP_MICROS => {
            TypeSignature::Timestamp
        }
        ConvertedType::DECIMAL => TypeSignature::Numeric,
        _ => match physical {
            parquet::basic::Type::BOOLEAN => TypeSignature::Bool,
            parquet::basic::Type::INT32 | parquet::basic::Type::INT64 => TypeSignature::Int8,
            parquet::basic::Type::FLOAT | parquet::basic::Type::DOUBLE => TypeSignature::Float8,
            parquet::basic::Type::INT96 => TypeSignature::Timestamp,
            _ => TypeSignature::Text,
        },
    }
}

async fn scan_single_parquet_file(
    storage: &IcebergStorage,
    file: &IcebergObjectLocation,
    output_fields: &[IcebergSchemaField],
    output_index: &HashMap<String, usize>,
    alias_map: &HashMap<String, String>,
) -> Result<Vec<Vec<ScalarValue>>, CatalogError> {
    let mut rows = Vec::new();
    if storage.kind == IcebergStorageKind::Local {
        let file = std::fs::File::open(file.path()).map_err(to_catalog_error)?;
        let reader = SerializedFileReader::new(file).map_err(to_catalog_error)?;
        read_parquet_rows(&reader, output_fields, output_index, alias_map, &mut rows)?;
        return Ok(rows);
    }

    let bytes = Bytes::from(storage.read_bytes(file).await?);
    let reader = SerializedFileReader::new(bytes).map_err(to_catalog_error)?;
    read_parquet_rows(&reader, output_fields, output_index, alias_map, &mut rows)?;
    Ok(rows)
}

fn read_parquet_rows<R>(
    reader: &SerializedFileReader<R>,
    output_fields: &[IcebergSchemaField],
    output_index: &HashMap<String, usize>,
    alias_map: &HashMap<String, String>,
    rows: &mut Vec<Vec<ScalarValue>>,
) -> Result<(), CatalogError>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    let row_iter = reader.get_row_iter(None).map_err(to_catalog_error)?;
    for row_result in row_iter {
        let row = row_result.map_err(to_catalog_error)?;
        let mut output_row = vec![ScalarValue::Null; output_fields.len()];
        for (physical_name, field) in row.get_column_iter() {
            let canonical = alias_map
                .get(&physical_name.to_ascii_lowercase())
                .cloned()
                .unwrap_or_else(|| physical_name.to_string());
            let Some(idx) = output_index.get(&canonical.to_ascii_lowercase()) else {
                continue;
            };
            let logical_field = &output_fields[*idx];
            output_row[*idx] = coerce_parquet_field(field, logical_field)?;
        }
        rows.push(output_row);
    }
    Ok(())
}

fn coerce_parquet_field(
    field: &ParquetField,
    logical_field: &IcebergSchemaField,
) -> Result<ScalarValue, CatalogError> {
    let scalar = parquet_field_to_scalar(field);
    if matches!(scalar, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    coerce_scalar_to_type(scalar, logical_field.type_signature)
}

fn parquet_field_to_scalar(field: &ParquetField) -> ScalarValue {
    match field {
        ParquetField::Null => ScalarValue::Null,
        ParquetField::Bool(value) => ScalarValue::Bool(*value),
        ParquetField::Byte(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::Short(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::Int(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::Long(value) => ScalarValue::Int(*value),
        ParquetField::UByte(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::UShort(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::UInt(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::ULong(value) => i64::try_from(*value)
            .map(ScalarValue::Int)
            .unwrap_or_else(|_| ScalarValue::Text(value.to_string())),
        ParquetField::Float16(value) => ScalarValue::Float(f64::from(*value)),
        ParquetField::Float(value) => ScalarValue::Float(f64::from(*value)),
        ParquetField::Double(value) => ScalarValue::Float(*value),
        ParquetField::Decimal(value) => ScalarValue::Text(parquet_decimal_to_text(value)),
        ParquetField::Str(value) => ScalarValue::Text(value.clone()),
        ParquetField::Bytes(value) => String::from_utf8(value.data().to_vec())
            .map(ScalarValue::Text)
            .unwrap_or_else(|_| {
                ScalarValue::Text(base64::prelude::BASE64_STANDARD.encode(value.data()))
            }),
        ParquetField::Date(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimeMillis(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimeMicros(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimestampMillis(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimestampMicros(value) => ScalarValue::Text(value.to_string()),
        ParquetField::Group(_) | ParquetField::ListInternal(_) | ParquetField::MapInternal(_) => {
            ScalarValue::Text(field.to_string())
        }
    }
}

fn parquet_decimal_to_text(value: &parquet::data_type::Decimal) -> String {
    let bytes = value.data();
    let scale = value.scale();
    if bytes.len() > 16 {
        return format!("{value:?}");
    }

    let sign_extension = if bytes.first().is_some_and(|byte| byte & 0x80 != 0) {
        0xFF
    } else {
        0x00
    };
    let mut padded = [sign_extension; 16];
    let start = 16 - bytes.len();
    padded[start..].copy_from_slice(bytes);
    let unscaled = i128::from_be_bytes(padded);
    format_scaled_i128(unscaled, scale)
}

fn format_scaled_i128(value: i128, scale: i32) -> String {
    if scale <= 0 {
        return value.to_string();
    }

    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs().to_string();
    let scale_usize = usize::try_from(scale).unwrap_or(0);
    if abs.len() <= scale_usize {
        let zeros = "0".repeat(scale_usize.saturating_sub(abs.len()));
        return format!("{sign}0.{zeros}{abs}");
    }

    let split = abs.len() - scale_usize;
    format!("{sign}{}.{}", &abs[..split], &abs[split..])
}

fn file_matches_partition_predicates(
    file: &IcebergDiscoveredFile,
    predicates: &[IcebergPartitionPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| evaluate_partition_predicate(predicate, &file.partition_values))
}

fn evaluate_partition_predicate(
    predicate: &IcebergPartitionPredicate,
    partition_values: &HashMap<String, ScalarValue>,
) -> bool {
    match predicate {
        IcebergPartitionPredicate::Eq(column, value) => compare_partition_value(
            partition_values.get(column),
            value,
            std::cmp::Ordering::Equal,
        ),
        IcebergPartitionPredicate::Lt(column, value) => compare_partition_value(
            partition_values.get(column),
            value,
            std::cmp::Ordering::Less,
        ),
        IcebergPartitionPredicate::Lte(column, value) => {
            partition_values.get(column).is_none_or(|partition| {
                let ordering = scalar_cmp(partition, value);
                ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
            })
        }
        IcebergPartitionPredicate::Gt(column, value) => partition_values
            .get(column)
            .is_none_or(|partition| scalar_cmp(partition, value) == std::cmp::Ordering::Greater),
        IcebergPartitionPredicate::Gte(column, value) => {
            partition_values.get(column).is_none_or(|partition| {
                let ordering = scalar_cmp(partition, value);
                ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
            })
        }
        IcebergPartitionPredicate::In(column, values) => {
            partition_values.get(column).is_none_or(|partition| {
                values
                    .iter()
                    .any(|value| scalar_cmp(partition, value) == std::cmp::Ordering::Equal)
            })
        }
        IcebergPartitionPredicate::Between(column, low, high) => {
            partition_values.get(column).is_none_or(|partition| {
                let low_cmp = scalar_cmp(partition, low);
                let high_cmp = scalar_cmp(partition, high);
                (low_cmp == std::cmp::Ordering::Greater || low_cmp == std::cmp::Ordering::Equal)
                    && (high_cmp == std::cmp::Ordering::Less
                        || high_cmp == std::cmp::Ordering::Equal)
            })
        }
    }
}

fn compare_partition_value(
    value: Option<&ScalarValue>,
    target: &ScalarValue,
    wanted: std::cmp::Ordering,
) -> bool {
    value.is_none_or(|value| scalar_cmp(value, target) == wanted)
}

fn extract_single_partition_predicate(
    expr: &Expr,
    qualifiers: &[String],
    partition_lookup: &HashMap<String, IcebergSchemaField>,
    params: &[Option<ScalarValue>],
) -> Result<Option<IcebergPartitionPredicate>, EngineError> {
    match expr {
        Expr::Binary { left, op, right } => {
            if let Some((column, field, value, reverse)) = extract_partition_column_and_value(
                left,
                right,
                qualifiers,
                partition_lookup,
                params,
            )? {
                let predicate = match (op.clone(), reverse) {
                    (BinaryOp::Eq, false | true) => IcebergPartitionPredicate::Eq(column, value),
                    (BinaryOp::Lt, false) => IcebergPartitionPredicate::Lt(column, value),
                    (BinaryOp::Lt, true) => IcebergPartitionPredicate::Gt(column, value),
                    (BinaryOp::Lte, false) => IcebergPartitionPredicate::Lte(column, value),
                    (BinaryOp::Lte, true) => IcebergPartitionPredicate::Gte(column, value),
                    (BinaryOp::Gt, false) => IcebergPartitionPredicate::Gt(column, value),
                    (BinaryOp::Gt, true) => IcebergPartitionPredicate::Lt(column, value),
                    (BinaryOp::Gte, false) => IcebergPartitionPredicate::Gte(column, value),
                    (BinaryOp::Gte, true) => IcebergPartitionPredicate::Lte(column, value),
                    _ => return Ok(None),
                };
                let _ = field;
                return Ok(Some(predicate));
            }
            Ok(None)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let Some((column, field)) =
                extract_partition_column(expr, qualifiers, partition_lookup)
            else {
                return Ok(None);
            };
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let scalar = eval_constant_scalar(item, params)?;
                values.push(
                    coerce_scalar_to_type(scalar, field.type_signature).map_err(to_engine_error)?,
                );
            }
            Ok(Some(IcebergPartitionPredicate::In(column, values)))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } if !negated => {
            let Some((column, field)) =
                extract_partition_column(expr, qualifiers, partition_lookup)
            else {
                return Ok(None);
            };
            let low =
                coerce_scalar_to_type(eval_constant_scalar(low, params)?, field.type_signature)
                    .map_err(to_engine_error)?;
            let high =
                coerce_scalar_to_type(eval_constant_scalar(high, params)?, field.type_signature)
                    .map_err(to_engine_error)?;
            Ok(Some(IcebergPartitionPredicate::Between(column, low, high)))
        }
        _ => Ok(None),
    }
}

fn extract_partition_column_and_value(
    left: &Expr,
    right: &Expr,
    qualifiers: &[String],
    partition_lookup: &HashMap<String, IcebergSchemaField>,
    params: &[Option<ScalarValue>],
) -> Result<Option<(String, IcebergSchemaField, ScalarValue, bool)>, EngineError> {
    if let Some((column, field)) = extract_partition_column(left, qualifiers, partition_lookup) {
        let value = eval_constant_scalar(right, params)?;
        let value = coerce_scalar_to_type(value, field.type_signature).map_err(to_engine_error)?;
        return Ok(Some((column, field, value, false)));
    }
    if let Some((column, field)) = extract_partition_column(right, qualifiers, partition_lookup) {
        let value = eval_constant_scalar(left, params)?;
        let value = coerce_scalar_to_type(value, field.type_signature).map_err(to_engine_error)?;
        return Ok(Some((column, field, value, true)));
    }
    Ok(None)
}

fn extract_partition_column(
    expr: &Expr,
    qualifiers: &[String],
    partition_lookup: &HashMap<String, IcebergSchemaField>,
) -> Option<(String, IcebergSchemaField)> {
    let Expr::Identifier(parts) = expr else {
        return None;
    };
    match parts.as_slice() {
        [column] => partition_lookup
            .get(&column.to_ascii_lowercase())
            .cloned()
            .map(|field| (field.name.to_ascii_lowercase(), field)),
        [qualifier, column]
            if qualifiers
                .iter()
                .any(|item| item.eq_ignore_ascii_case(qualifier)) =>
        {
            partition_lookup
                .get(&column.to_ascii_lowercase())
                .cloned()
                .map(|field| (field.name.to_ascii_lowercase(), field))
        }
        _ => None,
    }
}

fn eval_constant_scalar(
    expr: &Expr,
    params: &[Option<ScalarValue>],
) -> Result<ScalarValue, EngineError> {
    match expr {
        Expr::Null => Ok(ScalarValue::Null),
        Expr::Boolean(value) => Ok(ScalarValue::Bool(*value)),
        Expr::Integer(value) => Ok(ScalarValue::Int(*value)),
        Expr::Float(value) => Ok(ScalarValue::Float(value.parse::<f64>().map_err(|_| {
            EngineError {
                message: "unsupported partition predicate literal".to_string(),
            }
        })?)),
        Expr::String(value) => Ok(ScalarValue::Text(value.clone())),
        Expr::Unary {
            op: UnaryOp::Minus,
            expr,
        } => match eval_constant_scalar(expr, params)? {
            ScalarValue::Int(value) => Ok(ScalarValue::Int(-value)),
            ScalarValue::Float(value) => Ok(ScalarValue::Float(-value)),
            _ => Err(EngineError {
                message: "unsupported partition predicate literal".to_string(),
            }),
        },
        Expr::Unary {
            op: UnaryOp::Plus,
            expr,
        } => eval_constant_scalar(expr, params),
        Expr::Cast { expr, .. } => eval_constant_scalar(expr, params),
        Expr::Parameter(index) => params
            .get(index.saturating_sub(1) as usize)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| EngineError {
                message: format!("missing value for parameter ${index}"),
            }),
        _ => Err(EngineError {
            message: "unsupported partition predicate expression".to_string(),
        }),
    }
}

fn coerce_scalar_to_type(
    value: ScalarValue,
    type_signature: TypeSignature,
) -> Result<ScalarValue, CatalogError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    match (type_signature, value) {
        (TypeSignature::Bool, ScalarValue::Bool(value)) => Ok(ScalarValue::Bool(value)),
        (TypeSignature::Bool, ScalarValue::Text(value)) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Ok(ScalarValue::Bool(true)),
                "false" | "f" | "0" => Ok(ScalarValue::Bool(false)),
                _ => Err(catalog_error("invalid boolean literal")),
            }
        }
        (TypeSignature::Int8, ScalarValue::Int(value)) => Ok(ScalarValue::Int(value)),
        (TypeSignature::Int8, ScalarValue::Text(value)) => value
            .trim()
            .parse::<i64>()
            .map(ScalarValue::Int)
            .map_err(to_catalog_error),
        (TypeSignature::Float8, ScalarValue::Int(value)) => Ok(ScalarValue::Float(value as f64)),
        (TypeSignature::Float8, ScalarValue::Float(value)) => Ok(ScalarValue::Float(value)),
        (TypeSignature::Float8, ScalarValue::Text(value)) => value
            .trim()
            .parse::<f64>()
            .map(ScalarValue::Float)
            .map_err(to_catalog_error),
        (TypeSignature::Numeric, ScalarValue::Int(value)) => {
            Ok(ScalarValue::Numeric(rust_decimal::Decimal::from(value)))
        }
        (TypeSignature::Numeric, ScalarValue::Float(value)) => {
            use rust_decimal::prelude::FromPrimitive;
            Ok(ScalarValue::Numeric(
                rust_decimal::Decimal::from_f64(value)
                    .ok_or_else(|| catalog_error("invalid numeric literal"))?,
            ))
        }
        (TypeSignature::Numeric, ScalarValue::Numeric(value)) => Ok(ScalarValue::Numeric(value)),
        (TypeSignature::Numeric, ScalarValue::Text(value)) => value
            .trim()
            .parse::<rust_decimal::Decimal>()
            .map(ScalarValue::Numeric)
            .map_err(to_catalog_error),
        (TypeSignature::Text, ScalarValue::Text(value)) => Ok(ScalarValue::Text(value)),
        (TypeSignature::Text, value) => Ok(ScalarValue::Text(value.render())),
        (TypeSignature::Date, ScalarValue::Text(value)) => {
            Ok(ScalarValue::Text(value.trim().to_string()))
        }
        (TypeSignature::Timestamp, ScalarValue::Text(value)) => {
            Ok(ScalarValue::Text(value.trim().to_string()))
        }
        (TypeSignature::Vector(_), value) => Ok(ScalarValue::Text(value.render())),
        (_, value) => Err(catalog_error(format!(
            "unsupported Iceberg type coercion from {}",
            value.render()
        ))),
    }
}

fn coerce_text_to_type(
    value: &str,
    type_signature: TypeSignature,
) -> Result<ScalarValue, CatalogError> {
    coerce_scalar_to_type(ScalarValue::Text(value.to_string()), type_signature)
}

fn scalar_cmp(left: &ScalarValue, right: &ScalarValue) -> std::cmp::Ordering {
    match (left, right) {
        (ScalarValue::Int(left), ScalarValue::Int(right)) => left.cmp(right),
        (ScalarValue::Int(left), ScalarValue::Float(right)) => (*left as f64)
            .partial_cmp(right)
            .unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Float(left), ScalarValue::Int(right)) => left
            .partial_cmp(&(*right as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Float(left), ScalarValue::Float(right)) => {
            left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => left.render().cmp(&right.render()),
    }
}

fn decompose_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    let mut out = Vec::new();
    decompose_and_conjuncts_inner(expr, &mut out);
    out
}

fn decompose_and_conjuncts_inner(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = expr
    {
        decompose_and_conjuncts_inner(left, out);
        decompose_and_conjuncts_inner(right, out);
    } else {
        out.push(expr.clone());
    }
}

fn normalize_object_path(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    let normalized = value.replace('\\', "/");
    if normalized.starts_with('/') {
        normalized
    } else {
        normalized.trim_matches('/').to_string()
    }
}

fn render_location(kind: IcebergStorageKind, bucket: Option<&str>, path: &str) -> String {
    match kind {
        IcebergStorageKind::Local => path.to_string(),
        IcebergStorageKind::S3 => format!("s3://{}/{}", bucket.unwrap_or_default(), path),
        IcebergStorageKind::Gcs => format!("gs://{}/{}", bucket.unwrap_or_default(), path),
        IcebergStorageKind::AzureBlob => format!("az://{}/{}", bucket.unwrap_or_default(), path),
    }
}

fn object_path(path: &str) -> ObjectPath {
    ObjectPath::from(path.trim_matches('/'))
}

fn collect_local_files(directory: &Path, out: &mut Vec<String>) -> Result<(), CatalogError> {
    if directory.is_file() {
        out.push(directory.to_string_lossy().replace('\\', "/"));
        return Ok(());
    }
    if !directory.exists() {
        return Ok(());
    }
    for entry_result in std::fs::read_dir(directory).map_err(to_catalog_error)? {
        let entry = entry_result.map_err(to_catalog_error)?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(to_catalog_error)?;
        if file_type.is_dir() {
            collect_local_files(&path, out)?;
        } else if file_type.is_file() {
            out.push(path.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

fn to_catalog_error(error: impl std::fmt::Display) -> CatalogError {
    catalog_error(error.to_string())
}

fn to_engine_error(error: CatalogError) -> EngineError {
    EngineError {
        message: error.message,
    }
}

fn catalog_error(message: impl Into<String>) -> CatalogError {
    CatalogError {
        message: message.into(),
    }
}

#[derive(Debug)]
struct IcebergCatalogLayout {
    root_name: String,
    root_has_direct_tables: bool,
    distinct_first_segments: HashSet<String>,
}

#[derive(Debug, Clone)]
struct IcebergCatalogEntry {
    catalog_name: String,
    namespace_path: Vec<String>,
    table_name: String,
}

impl IcebergCatalogLayout {
    fn from_table_roots(root: &IcebergObjectLocation, tables: &[IcebergObjectLocation]) -> Self {
        let mut distinct_first_segments = HashSet::new();
        let mut root_has_direct_tables = false;
        for table in tables {
            let relative = table.relative_to(root).unwrap_or_default();
            let segments = split_path_segments(&relative);
            if segments.len() <= 1 {
                root_has_direct_tables = true;
            }
            if let Some(first) = segments.first() {
                distinct_first_segments.insert(first.clone());
            }
        }
        Self {
            root_name: root
                .file_name()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| "iceberg".to_string()),
            root_has_direct_tables,
            distinct_first_segments,
        }
    }

    fn entry_for(
        &self,
        root: &IcebergObjectLocation,
        table_root: &IcebergObjectLocation,
    ) -> IcebergCatalogEntry {
        let relative = table_root.relative_to(root).unwrap_or_default();
        let mut segments = split_path_segments(&relative);
        if segments.is_empty() {
            return IcebergCatalogEntry {
                catalog_name: self.root_name.clone(),
                namespace_path: Vec::new(),
                table_name: table_root
                    .file_name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| "table".to_string()),
            };
        }

        let use_first_segment_as_catalog = !self.root_has_direct_tables
            && self.distinct_first_segments.len() > 1
            && segments.len() >= 2;
        let catalog_name = if use_first_segment_as_catalog {
            segments.remove(0)
        } else {
            self.root_name.clone()
        };
        let table_name = segments
            .pop()
            .unwrap_or_else(|| table_root.file_name().unwrap_or("table").to_string());
        IcebergCatalogEntry {
            catalog_name,
            namespace_path: segments,
            table_name,
        }
    }

    fn catalog_location(&self, root: &IcebergObjectLocation, catalog_name: &str) -> String {
        if self.root_has_direct_tables || self.distinct_first_segments.len() <= 1 {
            root.raw().to_string()
        } else {
            root.join(catalog_name).raw().to_string()
        }
    }
}

fn split_path_segments(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_local_storage_locations() {
        let location =
            IcebergObjectLocation::parse("/tmp/demo/table").expect("local path should parse");
        assert_eq!(location.store_kind(), IcebergStorageKind::Local);
        assert_eq!(location.path(), "/tmp/demo/table");
    }

    #[test]
    fn parses_cloud_storage_locations() {
        let s3 =
            IcebergObjectLocation::parse("s3://warehouse/trades").expect("s3 uri should parse");
        assert_eq!(s3.store_kind(), IcebergStorageKind::S3);
        assert_eq!(s3.bucket(), Some("warehouse"));
        assert_eq!(s3.path(), "trades");

        let gcs =
            IcebergObjectLocation::parse("gs://lake/crypto/trades").expect("gs uri should parse");
        assert_eq!(gcs.store_kind(), IcebergStorageKind::Gcs);
        assert_eq!(gcs.bucket(), Some("lake"));

        let azure = IcebergObjectLocation::parse("az://container/root/table")
            .expect("azure uri should parse");
        assert_eq!(azure.store_kind(), IcebergStorageKind::AzureBlob);
        assert_eq!(azure.bucket(), Some("container"));
    }

    #[test]
    fn orders_metadata_versions() {
        assert!(compare_iceberg_metadata_files("v1.metadata.json", "v2.metadata.json").is_lt());
        assert_eq!(iceberg_metadata_version("v17.metadata.json"), 17);
    }

    #[test]
    fn layout_detects_multiple_catalogs() {
        let root = IcebergObjectLocation::parse("/tmp/warehouse").expect("root should parse");
        let tables = vec![
            IcebergObjectLocation::parse("/tmp/warehouse/alpha/crypto/trades")
                .expect("table should parse"),
            IcebergObjectLocation::parse("/tmp/warehouse/beta/market/ohlcv")
                .expect("table should parse"),
        ];
        let layout = IcebergCatalogLayout::from_table_roots(&root, &tables);
        let first = layout.entry_for(&root, &tables[0]);
        assert_eq!(first.catalog_name, "alpha");
        assert_eq!(first.namespace_path, vec!["crypto"]);
        assert_eq!(first.table_name, "trades");
    }

    #[test]
    fn partition_values_parse_from_hive_paths() {
        let data_root =
            IcebergObjectLocation::parse("/tmp/table/data").expect("data root should parse");
        let file = IcebergObjectLocation::parse(
            "/tmp/table/data/day=2024-01-01/symbol=btc/part-1.parquet",
        )
        .expect("file should parse");
        let values = parse_partition_values(
            &file,
            &data_root,
            &["day".to_string(), "symbol".to_string()],
            &HashMap::from([
                ("day".to_string(), TypeSignature::Date),
                ("symbol".to_string(), TypeSignature::Text),
            ]),
        );
        assert_eq!(
            values.get("day"),
            Some(&ScalarValue::Text("2024-01-01".to_string()))
        );
        assert_eq!(
            values.get("symbol"),
            Some(&ScalarValue::Text("btc".to_string()))
        );
    }
}
