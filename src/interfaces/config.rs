use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub source: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct DucklakeConfig {
    pub connection: String,
    pub alias: String,
    pub data_path: String,
    pub meta_schema: Option<String>,
    pub replace: Option<bool>,
    pub settings: Option<Vec<SettingConfig>>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct SecretConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub secret_type: String,
    pub key_id: Option<String>,
    pub secret: Option<String>,
    pub provider: Option<String>,
    pub region: Option<String>,
    pub token: Option<String>,
    pub scope: Option<String>,
    pub replace: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct SettingConfig {
    pub name: String,
    pub value: String,
}
