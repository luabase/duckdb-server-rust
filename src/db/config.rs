use anyhow::Result;
use duckdb::types::ToSql;
use duckdb::params_from_iter;
use tracing::log::info;

use crate::interfaces::{DucklakeConfig, Extension, SecretConfig, SettingConfig};

pub fn build_create_secret_query(secret_config: &SecretConfig) -> (String, Vec<Box<dyn ToSql>>) {
    let mut query = String::from(
        format!("CREATE OR REPLACE SECRET \"{}\" (TYPE ?", secret_config.name)
    );

    let mut params: Vec<Box<dyn ToSql>> = Vec::new();
    params.push(Box::new(secret_config.secret_type.clone()));

    if let Some(key_id) = &secret_config.key_id {
        query.push_str(", KEY_ID ?");
        params.push(Box::new(key_id.clone()));
    }
    if let Some(secret) = &secret_config.secret {
        query.push_str(", SECRET ?");
        params.push(Box::new(secret.clone()));
    }
    if let Some(provider) = &secret_config.provider {
        query.push_str(", PROVIDER ?");
        params.push(Box::new(provider.clone()));
    }
    if let Some(region) = &secret_config.region {
        query.push_str(", REGION ?");
        params.push(Box::new(region.clone()));
    }
    if let Some(token) = &secret_config.token {
        query.push_str(", TOKEN ?");
        params.push(Box::new(token.clone()));
    }
    if let Some(scope) = &secret_config.scope {
        query.push_str(", SCOPE ?");
        params.push(Box::new(scope.clone()));
    }
    query.push(')');
    (query, params)
}

pub fn build_attach_ducklake_query(ducklake_config: &DucklakeConfig) -> (String, Vec<Box<dyn ToSql>>) {
    let mut query = String::from(
        format!("ATTACH OR REPLACE '{}' AS \"{}\" (DATA_PATH ?", ducklake_config.connection, ducklake_config.alias)
    );
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();
    params.push(Box::new(ducklake_config.data_path.clone()));
    if let Some(meta_schema) = &ducklake_config.meta_schema {
        query.push_str(", META_SCHEMA ?");
        params.push(Box::new(meta_schema.clone()));
    }
    query.push(')');
    (query, params)
}

pub fn build_set_setting_query(setting_config: &SettingConfig) -> (String, Vec<Box<dyn ToSql>>) {
    let query = String::from(format!("SET {} = ?", setting_config.name));
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();
    params.push(Box::new(setting_config.value.clone()));
    (query, params)
}

pub fn load_extensions(conn: &duckdb::Connection, extensions: &[Extension]) -> Result<()> {
    if extensions.is_empty() {
        info!("No extensions to load");
        return Ok(());
    }

    let existing_extensions: Vec<_> = conn.prepare("SELECT extension_name, loaded, installed FROM duckdb_extensions()")?
        .query_arrow([])?
        .collect();

    let mut extension_map: std::collections::HashMap<String, (bool, bool)> = std::collections::HashMap::new();
    for batch in existing_extensions {
        let names = extract_string_column(&batch, 0, "extension_name")?;
        let loaded_array = batch.column(1).as_any().downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Expected BooleanArray for loaded column"))?;
        let installed_array = batch.column(2).as_any().downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Expected BooleanArray for installed column"))?;

        for (i, name) in names.into_iter().enumerate() {
            let loaded = loaded_array.value(i);
            let installed = installed_array.value(i);
            extension_map.insert(name, (loaded, installed));
        }
    }

    let mut commands = Vec::new();
    for ext in extensions {
        let (loaded, installed) = extension_map.get(&ext.name).copied().unwrap_or((false, false));

        if !installed {
            let install_sql = if let Some(source) = &ext.source {
                info!("Installing extension {} from source {}", ext.name, source);
                format!("INSTALL {} FROM {}", ext.name, source)
            }
            else {
                info!("Installing extension {}", ext.name);
                format!("INSTALL {}", ext.name)
            };
            commands.push(install_sql);
        }

        if !loaded {
            info!("Loading extension {}", ext.name);
            commands.push(format!("LOAD {}", ext.name));
        }
    }

    if !commands.is_empty() {
        let batch = commands.join(";\n");
        conn.execute_batch(&batch)?;
    }

    Ok(())
}

pub fn merge_secrets(existing: &Option<Vec<SecretConfig>>, incoming: &[SecretConfig]) -> Vec<SecretConfig> {
    let mut merged = existing.clone().unwrap_or_default();

    for incoming_secret in incoming {
        let replace = incoming_secret.replace.unwrap_or(false);
        let existing_index = merged.iter().position(|s| s.name == incoming_secret.name);

        match existing_index {
            Some(idx) if replace => {
                merged[idx] = incoming_secret.clone();
            }
            Some(_) => {
                // Skip if exists and replace is false
            }
            None => {
                merged.push(incoming_secret.clone());
            }
        }
    }

    merged
}

pub fn merge_ducklakes(existing: &Option<Vec<DucklakeConfig>>, incoming: &[DucklakeConfig]) -> Vec<DucklakeConfig> {
    let mut merged = existing.clone().unwrap_or_default();

    for incoming_ducklake in incoming {
        let replace = incoming_ducklake.replace.unwrap_or(false);
        let existing_index = merged.iter().position(|d| d.alias == incoming_ducklake.alias);

        match existing_index {
            Some(idx) if replace => {
                merged[idx] = incoming_ducklake.clone();
            }
            Some(_) => {
                // Skip if exists and replace is false
            }
            None => {
                merged.push(incoming_ducklake.clone());
            }
        }
    }

    merged
}

pub fn merge_extensions(existing: &Option<Vec<Extension>>, incoming: &[Extension]) -> Vec<Extension> {
    let mut merged = existing.clone().unwrap_or_default();

    for incoming_extension in incoming {
        let existing_index = merged.iter().position(|e| e.name == incoming_extension.name);

        match existing_index {
            Some(idx) => {
                merged[idx] = incoming_extension.clone();
            }
            None => {
                merged.push(incoming_extension.clone());
            }
        }
    }

    merged
}

pub fn extract_string_column(batch: &arrow::record_batch::RecordBatch, column_index: usize, column_name: &str) -> Result<Vec<String>> {
    let string_array = batch.column(column_index).as_any().downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| anyhow::anyhow!("Expected StringArray for {} column", column_name))?;

    let result = string_array.iter()
        .map(|opt| opt.unwrap_or("").to_string())
        .collect();
    Ok(result)
}

pub fn setup_secrets(conn: &duckdb::Connection, secrets: &[SecretConfig]) -> Result<()> {
    for secret in secrets {
        let (sql, args) = build_create_secret_query(secret);
        let mut stmt = conn.prepare(&sql)?;
        _ = stmt.execute(params_from_iter(args.iter()))?;

        info!("Created secret {} of type {}", secret.name, secret.secret_type);
    }

    Ok(())
}

pub fn setup_ducklakes(conn: &duckdb::Connection, ducklakes: &[DucklakeConfig]) -> Result<()> {
    let attached_lakes: Vec<_> = conn.prepare("PRAGMA database_list")?.query_arrow([])?.collect();
    let mut attached_names: Vec<String> = Vec::new();
    for batch in attached_lakes {
        let schema = batch.schema();
        let name_col_idx = schema.fields().iter().position(|f| f.name() == "name")
            .ok_or_else(|| anyhow::anyhow!("'name' column not found in database_list schema"))?;
        let names = extract_string_column(&batch, name_col_idx, "name")?;
        attached_names.extend(names);
    }

    for ducklake in ducklakes {
        let already_attached = attached_names.contains(&ducklake.alias);

        if already_attached && !ducklake.replace.unwrap_or(false) {
            continue;
        }

        let (sql, args) = build_attach_ducklake_query(ducklake);
        let mut stmt = conn.prepare(&sql)?;
        _ = stmt.execute(params_from_iter(args.iter()))?;

        if let Some(settings) = &ducklake.settings {
            for setting in settings {
                let (sql, args) = build_set_setting_query(setting);
                let mut stmt = conn.prepare(&sql)?;
                _ = stmt.execute(params_from_iter(args.iter()))?;
            }
        }

        info!("Attached ducklake {}", ducklake.alias);
    }

    Ok(())
}
