use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub source: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
pub struct DucklakeConfig {
    pub connection: String,
    pub alias: String,
    pub data_path: String,
    pub meta_schema: Option<String>,
    pub replace: Option<bool>,
    pub settings: Option<Vec<SettingConfig>>,
}

impl std::fmt::Debug for DucklakeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DucklakeConfig")
            .field("connection", &"[REDACTED]")
            .field("alias", &self.alias)
            .field("data_path", &self.data_path)
            .field("meta_schema", &self.meta_schema)
            .field("replace", &self.replace)
            .field("settings", &self.settings)
            .finish()
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
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

impl std::fmt::Debug for SecretConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretConfig")
            .field("name", &self.name)
            .field("secret_type", &self.secret_type)
            .field("key_id", &self.key_id.as_ref().map(|_| "[REDACTED]"))
            .field("secret", &self.secret.as_ref().map(|_| "[REDACTED]"))
            .field("provider", &self.provider)
            .field("region", &self.region)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("scope", &self.scope)
            .field("replace", &self.replace)
            .finish()
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct SettingConfig {
    pub name: String,
    pub value: String,
}
