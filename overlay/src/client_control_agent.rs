use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use hbb_common::{
    config::{keys, Config, LocalConfig},
    log,
    socket_client::connect_tcp,
    tls::TlsType,
    tokio::{self, time::MissedTickBehavior},
};
use reqwest::Method;
use serde_json::{json, Value};

const ENV_ENABLED: &str = "RDX_CLIENT_CTRL_ENABLED";
const ENV_BASE_URL: &str = "RDX_CLIENT_CTRL_BASE_URL";
const ENV_BOOTSTRAP_BASE_URL: &str = "RDX_RUNTIME_BOOTSTRAP_BASE_URL";
const ENV_ENROLL_KEY: &str = "RDX_CLIENT_CTRL_ENROLL_KEY";
const ENV_DEVICE_ID: &str = "RDX_CLIENT_CTRL_DEVICE_ID";
const ENV_DEVICE_NAME: &str = "RDX_CLIENT_CTRL_DEVICE_NAME";
const ENV_GROUP_ID: &str = "RDX_CLIENT_CTRL_GROUP_ID";
const ENV_TAGS: &str = "RDX_CLIENT_CTRL_TAGS";
const ENV_PROFILE_ID: &str = "RDX_CLIENT_CTRL_PROFILE_ID";
const ENV_NOTE: &str = "RDX_CLIENT_CTRL_NOTE";
const ENV_DEVICE_TOKEN: &str = "RDX_CLIENT_CTRL_DEVICE_TOKEN";
const ENV_RUNTIME_PACKAGE_CODE: &str = "RDX_RUNTIME_PACKAGE_CODE";
const ENV_RUNTIME_USERNAME: &str = "RDX_RUNTIME_USERNAME";
const ENV_RUNTIME_PASSWORD: &str = "RDX_RUNTIME_PASSWORD";
const ENV_MANUAL_NODE_CODE: &str = "RDX_RUNTIME_MANUAL_NODE_CODE";
const ENV_BOOTSTRAP_SEC: &str = "RDX_RUNTIME_BOOTSTRAP_SEC";
const ENV_HEARTBEAT_SEC: &str = "RDX_CLIENT_CTRL_HEARTBEAT_SEC";
const ENV_CONFIG_SEC: &str = "RDX_CLIENT_CTRL_CONFIG_SEC";
const ENV_PULL_SEC: &str = "RDX_CLIENT_CTRL_PULL_SEC";

const OPT_BASE_URL: &str = "rdx-client-control-base-url";
const OPT_BOOTSTRAP_BASE_URL: &str = "rdx-runtime-bootstrap-base-url";
const OPT_ENROLL_KEY: &str = "rdx-client-control-enroll-key";
const OPT_DEVICE_ID: &str = "rdx-client-control-device-id";
const OPT_DEVICE_NAME: &str = "rdx-client-control-device-name";
const OPT_GROUP_ID: &str = "rdx-client-control-group-id";
const OPT_TAGS: &str = "rdx-client-control-tags";
const OPT_PROFILE_ID: &str = "rdx-client-control-profile-id";
const OPT_NOTE: &str = "rdx-client-control-note";
const OPT_DEVICE_TOKEN: &str = "rdx-client-control-device-token";
const OPT_RUNTIME_PACKAGE_CODE: &str = "rdx-runtime-package-code";
const OPT_RUNTIME_USERNAME: &str = "rdx-runtime-username";
const OPT_RUNTIME_PASSWORD: &str = "rdx-runtime-password";
const OPT_RUNTIME_ACCESS_TOKEN: &str = "rdx-runtime-access-token";
const OPT_MANUAL_NODE_CODE: &str = "rdx-runtime-manual-node-code";
const OPT_SELECTED_NODE_CODE: &str = "rdx-runtime-selected-node-code";
const OPT_BOOTSTRAP_SEC: &str = "rdx-runtime-bootstrap-sec";
const OPT_RUNTIME_RENDEZVOUS_SERVER: &str = "rdx-runtime-custom-rendezvous-server";
const OPT_RUNTIME_API_SERVER: &str = "rdx-runtime-api-server";
const OPT_RUNTIME_RELAY_SERVER: &str = "rdx-runtime-relay-server";
const OPT_RUNTIME_SELECTED_NODE_PROBE_HOST: &str = "rdx-runtime-selected-node-probe-host";
const OPT_RUNTIME_SELECTED_NODE_RELAY_SERVER: &str = "rdx-runtime-selected-node-relay-server";
const OPT_RUNTIME_SELECTED_NODE_API_SERVER: &str = "rdx-runtime-selected-node-api-server";
const OPT_RUNTIME_FIXED_ID_SERVER: &str = "rdx-runtime-fixed-id-server";
const OPT_RUNTIME_FIXED_API_SERVER: &str = "rdx-runtime-fixed-api-server";
const OPT_RUNTIME_FIXED_PUBLIC_KEY: &str = "rdx-runtime-fixed-public-key";
const OPT_RUNTIME_EXPIRES_AT: &str = "rdx-runtime-expires-at";
const OPT_RUNTIME_PROFILE_NAME: &str = "rdx-runtime-profile-name";
const OPT_RUNTIME_SERVICE_NODES: &str = "rdx-runtime-service-nodes";
const OPT_RUNTIME_AUTO_SELECT_MODE: &str = "rdx-runtime-auto-select-mode";
const OPT_RUNTIME_ALLOW_MANUAL_SELECTION: &str = "rdx-runtime-allow-manual-selection";
const OPT_RUNTIME_SELECTED_NODE_LATENCY: &str = "rdx-runtime-selected-node-latency";
const OPT_RUNTIME_ASSIGNED_NODE_CODES: &str = "rdx-runtime-assigned-node-codes";
const OPT_RUNTIME_CLIENT_OPTIONS: &str = "rdx-runtime-client-options";
const OPT_RUNTIME_FIXED_PASSWORD: &str = "rdx-runtime-fixed-password";
const OPT_RUNTIME_DEVICE_ENABLED: &str = "rdx-runtime-device-enabled";
const OPT_RUNTIME_DEVICE_OWNER: &str = "rdx-runtime-device-owner";
const OPT_RUNTIME_TENANT_ID: &str = "rdx-runtime-tenant-id";
const OPT_RUNTIME_TENANT_NAME: &str = "rdx-runtime-tenant-name";
const OPT_RUNTIME_TENANT_STATUS: &str = "rdx-runtime-tenant-status";
const OPT_RUNTIME_BILLING_STATE: &str = "rdx-runtime-billing-state";
const OPT_RUNTIME_SERVER_LOCK_MODE: &str = "rdx-runtime-server-lock-mode";
const OPT_HEARTBEAT_SEC: &str = "rdx-client-control-heartbeat-sec";
const OPT_CONFIG_SEC: &str = "rdx-client-control-config-sec";
const OPT_PULL_SEC: &str = "rdx-client-control-pull-sec";

const DEFAULT_HEARTBEAT_SEC: u64 = 20;
const DEFAULT_CONFIG_SEC: u64 = 30;
const DEFAULT_PULL_SEC: u64 = 6;
const DEFAULT_BOOTSTRAP_SEC: u64 = 60;
const LATENCY_PROBE_TIMEOUT_MS: u64 = 1500;

#[derive(Clone, Debug)]
struct AgentConfig {
    base_url: String,
    bootstrap_base_url: String,
    enroll_key: String,
    device_id: String,
    device_name: String,
    group_id: String,
    tags: String,
    profile_id: String,
    note: String,
    runtime_package_code: String,
    runtime_username: String,
    runtime_password: String,
    bootstrap_sec: u64,
    legacy_control_enabled: bool,
    bootstrap_enabled: bool,
    heartbeat_sec: u64,
    config_sec: u64,
    pull_sec: u64,
}

#[derive(Debug)]
struct AgentRuntime {
    cfg: AgentConfig,
    device_token: String,
    runtime_access_token: String,
    last_config_fingerprint: String,
    last_bootstrap_fingerprint: String,
}

static AGENT_STARTED: AtomicBool = AtomicBool::new(false);

pub fn maybe_start() {
    if AGENT_STARTED.load(Ordering::Acquire) {
        return;
    }
    let Some(cfg) = AgentConfig::load() else {
        return;
    };
    if AGENT_STARTED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    log::info!(
        "client-control-agent enabled, base_url={}, bootstrap_base_url={}, package_code={}, device_id={}",
        cfg.base_url,
        cfg.bootstrap_base_url,
        cfg.runtime_package_code,
        cfg.device_id
    );
    tokio::spawn(async move {
        if let Err(err) = run_agent_forever(cfg).await {
            log::error!("client-control-agent stopped: {}", err);
        }
        AGENT_STARTED.store(false, Ordering::Release);
    });
}

pub fn trigger_runtime_bootstrap() {
    maybe_start();
    tokio::spawn(async move {
        prime_runtime_bootstrap().await;
    });
}

pub async fn prime_runtime_bootstrap() {
    let Some(cfg) = AgentConfig::load() else {
        return;
    };
    if !cfg.bootstrap_enabled {
        return;
    }

    let client = crate::hbbs_http::create_http_client_async(TlsType::Rustls, false);
    let mut runtime = AgentRuntime {
        device_token: String::new(),
        runtime_access_token: read_option_trimmed(OPT_RUNTIME_ACCESS_TOKEN).unwrap_or_default(),
        cfg,
        last_config_fingerprint: String::new(),
        last_bootstrap_fingerprint: String::new(),
    };
    if let Err(err) = do_runtime_bootstrap(&client, &mut runtime).await {
        log::warn!("client-control-agent initial bootstrap failed: {}", err);
    }
}

impl AgentConfig {
    fn load() -> Option<Self> {
        if let Some(v) = read_env_trimmed(ENV_ENABLED) {
            let v = v.to_ascii_lowercase();
            if matches!(v.as_str(), "0" | "n" | "no" | "false" | "off") {
                log::info!("client-control-agent disabled by {}", ENV_ENABLED);
                return None;
            }
        }

        let mut base_url = read_env_trimmed(ENV_BASE_URL)
            .or_else(|| read_option_trimmed(OPT_BASE_URL))
            .or_else(derive_control_base_from_runtime)
            .unwrap_or_default();
        base_url = normalize_base_url(&base_url);
        let mut bootstrap_base_url = read_env_trimmed(ENV_BOOTSTRAP_BASE_URL)
            .or_else(|| read_option_trimmed(OPT_BOOTSTRAP_BASE_URL))
            .or_else(|| {
                if !base_url.is_empty() {
                    Some(base_url.clone())
                } else {
                    derive_control_base_from_runtime()
                }
            })
            .unwrap_or_default();
        bootstrap_base_url = normalize_base_url(&bootstrap_base_url);

        let enroll_key = read_env_trimmed(ENV_ENROLL_KEY)
            .or_else(|| read_option_trimmed(OPT_ENROLL_KEY))
            .unwrap_or_default();

        let existing_token = read_env_trimmed(ENV_DEVICE_TOKEN)
            .or_else(|| read_option_trimmed(OPT_DEVICE_TOKEN))
            .unwrap_or_default();
        let runtime_package_code = read_env_trimmed(ENV_RUNTIME_PACKAGE_CODE)
            .or_else(|| read_option_trimmed(OPT_RUNTIME_PACKAGE_CODE))
            .unwrap_or_default();
        let runtime_username = read_env_trimmed(ENV_RUNTIME_USERNAME)
            .or_else(|| read_option_trimmed(OPT_RUNTIME_USERNAME))
            .unwrap_or_default();
        let runtime_password = read_env_trimmed(ENV_RUNTIME_PASSWORD)
            .or_else(|| read_option_trimmed(OPT_RUNTIME_PASSWORD))
            .unwrap_or_default();
        let legacy_control_enabled =
            !base_url.is_empty() && (!enroll_key.is_empty() || !existing_token.is_empty());
        let runtime_login_enabled =
            !runtime_username.trim().is_empty() && !runtime_password.trim().is_empty();
        let bootstrap_enabled = !bootstrap_base_url.is_empty()
            && (runtime_login_enabled || !runtime_package_code.trim().is_empty());
        if !legacy_control_enabled && !bootstrap_enabled {
            log::debug!(
                "client-control-agent disabled: no usable legacy control credentials and no runtime bootstrap package"
            );
            return None;
        }

        let device_id = read_env_trimmed(ENV_DEVICE_ID)
            .or_else(|| read_option_trimmed(OPT_DEVICE_ID))
            .filter(|x| !x.is_empty())
            .unwrap_or_else(Config::get_id);

        if device_id.trim().is_empty() {
            log::warn!("client-control-agent disabled: device id is empty");
            return None;
        }

        let device_name = read_env_trimmed(ENV_DEVICE_NAME)
            .or_else(|| read_option_trimmed(OPT_DEVICE_NAME))
            .unwrap_or_else(hbb_common::whoami::hostname);

        Some(Self {
            base_url,
            bootstrap_base_url,
            enroll_key,
            device_id,
            device_name,
            group_id: read_env_trimmed(ENV_GROUP_ID)
                .or_else(|| read_option_trimmed(OPT_GROUP_ID))
                .unwrap_or_default(),
            tags: read_env_trimmed(ENV_TAGS)
                .or_else(|| read_option_trimmed(OPT_TAGS))
                .unwrap_or_default(),
            profile_id: read_env_trimmed(ENV_PROFILE_ID)
                .or_else(|| read_option_trimmed(OPT_PROFILE_ID))
                .unwrap_or_default(),
            note: read_env_trimmed(ENV_NOTE)
                .or_else(|| read_option_trimmed(OPT_NOTE))
                .unwrap_or_default(),
            runtime_package_code,
            runtime_username,
            runtime_password,
            bootstrap_sec: read_u64(ENV_BOOTSTRAP_SEC, OPT_BOOTSTRAP_SEC, DEFAULT_BOOTSTRAP_SEC)
                .clamp(10, 3600),
            legacy_control_enabled,
            bootstrap_enabled,
            heartbeat_sec: read_u64(ENV_HEARTBEAT_SEC, OPT_HEARTBEAT_SEC, DEFAULT_HEARTBEAT_SEC)
                .clamp(5, 3600),
            config_sec: read_u64(ENV_CONFIG_SEC, OPT_CONFIG_SEC, DEFAULT_CONFIG_SEC).clamp(5, 3600),
            pull_sec: read_u64(ENV_PULL_SEC, OPT_PULL_SEC, DEFAULT_PULL_SEC).clamp(2, 3600),
        })
    }

    fn runtime_login_enabled(&self) -> bool {
        !self.runtime_username.trim().is_empty() && !self.runtime_password.trim().is_empty()
    }

    fn refresh_runtime_config(&mut self) {
        let mut base_url = read_env_trimmed(ENV_BASE_URL)
            .or_else(|| read_option_trimmed(OPT_BASE_URL))
            .or_else(derive_control_base_from_runtime)
            .unwrap_or_default();
        base_url = normalize_base_url(&base_url);
        let mut bootstrap_base_url = read_env_trimmed(ENV_BOOTSTRAP_BASE_URL)
            .or_else(|| read_option_trimmed(OPT_BOOTSTRAP_BASE_URL))
            .or_else(|| {
                if !base_url.is_empty() {
                    Some(base_url.clone())
                } else {
                    derive_control_base_from_runtime()
                }
            })
            .unwrap_or_default();
        bootstrap_base_url = normalize_base_url(&bootstrap_base_url);

        self.base_url = base_url;
        self.bootstrap_base_url = bootstrap_base_url;
        self.runtime_package_code = read_env_trimmed(ENV_RUNTIME_PACKAGE_CODE)
            .or_else(|| read_option_trimmed(OPT_RUNTIME_PACKAGE_CODE))
            .unwrap_or_default();
        self.runtime_username = read_env_trimmed(ENV_RUNTIME_USERNAME)
            .or_else(|| read_option_trimmed(OPT_RUNTIME_USERNAME))
            .unwrap_or_default();
        self.runtime_password = read_env_trimmed(ENV_RUNTIME_PASSWORD)
            .or_else(|| read_option_trimmed(OPT_RUNTIME_PASSWORD))
            .unwrap_or_default();
        self.bootstrap_sec = read_u64(ENV_BOOTSTRAP_SEC, OPT_BOOTSTRAP_SEC, DEFAULT_BOOTSTRAP_SEC)
            .clamp(10, 3600);
        self.bootstrap_enabled = !self.bootstrap_base_url.is_empty()
            && (self.runtime_login_enabled() || !self.runtime_package_code.trim().is_empty());
    }
}

impl AgentRuntime {
    fn refresh_dynamic_state(&mut self) {
        self.cfg.refresh_runtime_config();
        self.runtime_access_token = read_option_trimmed(OPT_RUNTIME_ACCESS_TOKEN).unwrap_or_default();
        self.device_token = read_env_trimmed(ENV_DEVICE_TOKEN)
            .or_else(|| read_option_trimmed(OPT_DEVICE_TOKEN))
            .unwrap_or_default();
    }
}

async fn run_agent_forever(cfg: AgentConfig) -> Result<(), String> {
    let client = crate::hbbs_http::create_http_client_async(TlsType::Rustls, false);
    let mut runtime = AgentRuntime {
        device_token: read_env_trimmed(ENV_DEVICE_TOKEN)
            .or_else(|| read_option_trimmed(OPT_DEVICE_TOKEN))
            .unwrap_or_default(),
        runtime_access_token: read_option_trimmed(OPT_RUNTIME_ACCESS_TOKEN).unwrap_or_default(),
        cfg,
        last_config_fingerprint: String::new(),
        last_bootstrap_fingerprint: String::new(),
    };
    runtime.refresh_dynamic_state();

    if runtime.cfg.legacy_control_enabled {
        ensure_enrolled(&client, &mut runtime).await?;
    }
    if runtime.cfg.bootstrap_enabled {
        if let Err(err) = do_runtime_bootstrap(&client, &mut runtime).await {
            log::warn!("client-control-agent bootstrap failed: {}", err);
        }
    }

    let mut heartbeat_tick =
        tokio::time::interval(std::time::Duration::from_secs(runtime.cfg.heartbeat_sec));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut config_tick =
        tokio::time::interval(std::time::Duration::from_secs(runtime.cfg.config_sec));
    config_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut pull_tick = tokio::time::interval(std::time::Duration::from_secs(runtime.cfg.pull_sec));
    pull_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut bootstrap_interval_sec = runtime.cfg.bootstrap_sec;
    let mut bootstrap_tick = tokio::time::interval(Duration::from_secs(bootstrap_interval_sec));
    bootstrap_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        runtime.refresh_dynamic_state();
        if runtime.cfg.bootstrap_sec != bootstrap_interval_sec {
            bootstrap_interval_sec = runtime.cfg.bootstrap_sec;
            bootstrap_tick = tokio::time::interval(Duration::from_secs(bootstrap_interval_sec));
            bootstrap_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        }
        if !runtime.cfg.legacy_control_enabled && !runtime.cfg.bootstrap_enabled {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        tokio::select! {
            _ = heartbeat_tick.tick(), if runtime.cfg.legacy_control_enabled => {
                if let Err(err) = do_heartbeat(&client, &runtime).await {
                    handle_auth_or_log(&client, &mut runtime, "heartbeat", err).await;
                }
            }
            _ = config_tick.tick(), if runtime.cfg.legacy_control_enabled => {
                if let Err(err) = do_config(&client, &mut runtime).await {
                    handle_auth_or_log(&client, &mut runtime, "config", err).await;
                }
            }
            _ = pull_tick.tick(), if runtime.cfg.legacy_control_enabled => {
                if let Err(err) = do_pull_and_ack(&client, &mut runtime).await {
                    handle_auth_or_log(&client, &mut runtime, "pull", err).await;
                }
            }
            _ = bootstrap_tick.tick(), if runtime.cfg.bootstrap_enabled => {
                if let Err(err) = do_runtime_bootstrap(&client, &mut runtime).await {
                    log::warn!("client-control-agent bootstrap refresh failed: {}", err);
                }
            }
        }
    }
}

async fn handle_auth_or_log(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
    stage: &str,
    err: String,
) {
    if is_auth_error(&err) {
        log::warn!(
            "client-control-agent {} got auth error, re-enrolling",
            stage
        );
        runtime.device_token.clear();
        LocalConfig::set_option(OPT_DEVICE_TOKEN.to_owned(), "".to_owned());
        if let Err(e) = ensure_enrolled(client, runtime).await {
            log::warn!("client-control-agent re-enroll failed: {}", e);
        }
    } else {
        log::warn!("client-control-agent {} failed: {}", stage, err);
    }
}

async fn ensure_enrolled(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
) -> Result<(), String> {
    if !runtime.device_token.trim().is_empty() {
        return Ok(());
    }
    if runtime.cfg.enroll_key.trim().is_empty() {
        return Err("enroll key is empty".to_owned());
    }

    let url = format!("{}/api/v1/client/enroll", runtime.cfg.base_url);
    let body = json!({
        "device_id": runtime.cfg.device_id,
        "name": runtime.cfg.device_name,
        "group_id": runtime.cfg.group_id,
        "tags": runtime.cfg.tags,
        "os": std::env::consts::OS,
        "version": crate::VERSION,
        "profile_id": runtime.cfg.profile_id,
        "note": runtime.cfg.note,
    });

    let data = request_json(
        client,
        Method::POST,
        &url,
        vec![("x-client-enroll-key", runtime.cfg.enroll_key.clone())],
        Some(body),
    )
    .await?;

    let token = data
        .get("device_token")
        .and_then(|x| x.as_str())
        .map(str::trim)
        .unwrap_or_default()
        .to_owned();

    if token.is_empty() {
        return Err(format!(
            "enroll succeeded but device_token is empty: {}",
            data
        ));
    }

    runtime.device_token = token.clone();
    LocalConfig::set_option(OPT_DEVICE_TOKEN.to_owned(), token);
    log::info!(
        "client-control-agent enrolled, device_id={}",
        runtime.cfg.device_id
    );
    Ok(())
}

async fn do_heartbeat(client: &reqwest::Client, runtime: &AgentRuntime) -> Result<(), String> {
    let url = format!(
        "{}/api/v1/client/devices/{}/heartbeat",
        runtime.cfg.base_url, runtime.cfg.device_id
    );
    let body = json!({
        "status": "online",
        "os": std::env::consts::OS,
        "version": crate::VERSION,
    });
    request_json(
        client,
        Method::POST,
        &url,
        vec![("x-device-token", runtime.device_token.clone())],
        Some(body),
    )
    .await
    .map(|_| ())
}

async fn do_config(client: &reqwest::Client, runtime: &mut AgentRuntime) -> Result<(), String> {
    let url = format!(
        "{}/api/v1/client/devices/{}/config",
        runtime.cfg.base_url, runtime.cfg.device_id
    );
    let data = request_json(
        client,
        Method::GET,
        &url,
        vec![("x-device-token", runtime.device_token.clone())],
        None,
    )
    .await?;

    let desired_version = data
        .get("desired_version")
        .and_then(|x| x.as_str())
        .unwrap_or_default();
    let desired_channel = data
        .get("desired_channel")
        .and_then(|x| x.as_str())
        .unwrap_or_default();
    let effective_policy = data
        .get("effective_policy")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let fingerprint = format!(
        "{}|{}|{}",
        desired_version, desired_channel, effective_policy
    );
    if fingerprint != runtime.last_config_fingerprint {
        runtime.last_config_fingerprint = fingerprint;
        log::info!(
            "client-control-agent config updated: desired_version={}, desired_channel={}, policy_keys={}",
            desired_version,
            desired_channel,
            effective_policy.as_object().map(|x| x.len()).unwrap_or(0)
        );
    }
    Ok(())
}

async fn do_runtime_bootstrap(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
) -> Result<(), String> {
    if !runtime.cfg.bootstrap_enabled {
        return Ok(());
    }

    if runtime.cfg.runtime_login_enabled() {
        match do_runtime_assignment(client, runtime).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if runtime.cfg.runtime_package_code.trim().is_empty() {
                    return Err(err);
                }
                log::warn!(
                    "client-control-agent runtime-assignment failed, fallback to package bootstrap: {}",
                    err
                );
            }
        }
    }

    do_runtime_package_bootstrap(client, runtime).await
}

async fn do_runtime_package_bootstrap(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
) -> Result<(), String> {
    let url = format!(
        "{}/api/runtime-packages/bootstrap/{}",
        runtime.cfg.bootstrap_base_url, runtime.cfg.runtime_package_code
    );
    let root = request_json(client, Method::GET, &url, vec![], None).await?;
    let data = unwrap_api_data(root);
    apply_runtime_payload(runtime, &data, "bootstrap").await
}

async fn do_runtime_assignment(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
) -> Result<(), String> {
    ensure_runtime_login(client, runtime).await?;
    let root = match request_json(
        client,
        Method::GET,
        &format!(
            "{}/api/client/runtime-assignment",
            runtime.cfg.bootstrap_base_url
        ),
        vec![
            (
                "Authorization",
                format!("Bearer {}", runtime.runtime_access_token),
            ),
            ("x-rustdesk-id", runtime.cfg.device_id.clone()),
            ("x-rustdesk-uuid", runtime_device_uuid()),
        ],
        None,
    )
    .await
    {
        Ok(root) => root,
        Err(err) => {
            if !is_auth_error(&err) {
                return Err(err);
            }
            runtime.runtime_access_token.clear();
            LocalConfig::set_option(OPT_RUNTIME_ACCESS_TOKEN.to_owned(), String::new());
            ensure_runtime_login(client, runtime).await?;
            request_json(
                client,
                Method::GET,
                &format!(
                    "{}/api/client/runtime-assignment",
                    runtime.cfg.bootstrap_base_url
                ),
                vec![
                    (
                        "Authorization",
                        format!("Bearer {}", runtime.runtime_access_token),
                    ),
                    ("x-rustdesk-id", runtime.cfg.device_id.clone()),
                    ("x-rustdesk-uuid", runtime_device_uuid()),
                ],
                None,
            )
            .await?
        }
    };
    let data = unwrap_api_data(root);
    apply_runtime_payload(runtime, &data, "assignment").await
}

async fn ensure_runtime_login(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
) -> Result<(), String> {
    if !runtime.runtime_access_token.trim().is_empty() {
        return Ok(());
    }
    if !runtime.cfg.runtime_login_enabled() {
        return Err("runtime login credentials are empty".to_owned());
    }

    let root = request_json(
        client,
        Method::POST,
        &format!("{}/api/client/auth/login", runtime.cfg.bootstrap_base_url),
        vec![],
        Some(json!({
            "username": runtime.cfg.runtime_username.clone(),
            "password": runtime.cfg.runtime_password.clone(),
            "deviceId": runtime.cfg.device_id.clone(),
            "deviceUuid": runtime_device_uuid(),
        })),
    )
    .await?;
    let data = unwrap_api_data(root);
    let access_token = value_string(data.get("accessToken"));
    if access_token.is_empty() {
        return Err("runtime login succeeded but accessToken is empty".to_owned());
    }

    runtime.runtime_access_token = access_token.clone();
    LocalConfig::set_option(OPT_RUNTIME_ACCESS_TOKEN.to_owned(), access_token);

    let bootstrap_base_url = normalize_base_url(&value_string(data.get("bootstrapBaseUrl")));
    if !bootstrap_base_url.is_empty() {
        runtime.cfg.bootstrap_base_url = bootstrap_base_url.clone();
        LocalConfig::set_option(OPT_BOOTSTRAP_BASE_URL.to_owned(), bootstrap_base_url);
    }
    let package_code = value_string(data.get("packageCode"));
    if !package_code.is_empty() {
        runtime.cfg.runtime_package_code = package_code.clone();
        LocalConfig::set_option(OPT_RUNTIME_PACKAGE_CODE.to_owned(), package_code);
    }
    let profile_name = value_string(data.get("profileName"));
    if !profile_name.is_empty() {
        LocalConfig::set_option(OPT_RUNTIME_PROFILE_NAME.to_owned(), profile_name);
    }

    Ok(())
}

async fn apply_runtime_payload(
    runtime: &mut AgentRuntime,
    data: &Value,
    source: &str,
) -> Result<(), String> {
    let package_code = value_string(data.get("packageCode"));
    if !package_code.is_empty() {
        runtime.cfg.runtime_package_code = package_code.clone();
        LocalConfig::set_option(OPT_RUNTIME_PACKAGE_CODE.to_owned(), package_code);
    }
    let bootstrap_base_url = normalize_base_url(&value_string(data.get("bootstrapBaseUrl")));
    if !bootstrap_base_url.is_empty() {
        runtime.cfg.bootstrap_base_url = bootstrap_base_url.clone();
        LocalConfig::set_option(OPT_BOOTSTRAP_BASE_URL.to_owned(), bootstrap_base_url);
    }

    let service_nodes = data
        .get("serviceNodes")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    if service_nodes.is_empty() {
        return Err("bootstrap payload missing serviceNodes".to_owned());
    }

    let allow_manual_selection = data
        .get("allowManualSelection")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    persist_runtime_assignment_state(data)?;
    persist_runtime_service_nodes(&data, &service_nodes, allow_manual_selection)?;
    let auto_select_mode = value_string(data.get("autoSelectMode"));
    let (selected, selected_latency) =
        select_service_node(&service_nodes, allow_manual_selection, &auto_select_mode)
            .await
            .ok_or_else(|| "no selectable service node found in bootstrap payload".to_owned())?;

    apply_runtime_service_node(&data, &selected, selected_latency)?;

    let selected_code = value_string(selected.get("code"));
    let fingerprint = format!(
        "{}|{}|{}|{}|{}|{}",
        source,
        runtime.cfg.runtime_package_code,
        selected_code,
        value_string(data.get("expiresAt")),
        auto_select_mode,
        service_nodes.len()
    );
    if fingerprint != runtime.last_bootstrap_fingerprint {
        runtime.last_bootstrap_fingerprint = fingerprint;
        log::info!(
            "client-control-agent bootstrap applied: package_code={}, selected_node={}, latency_ms={}, allow_manual_selection={}, auto_select_mode={}, candidates={}",
            runtime.cfg.runtime_package_code,
            selected_code,
            selected_latency.unwrap_or_default(),
            allow_manual_selection,
            auto_select_mode,
            service_nodes.len()
        );
    }

    Ok(())
}

async fn do_pull_and_ack(
    client: &reqwest::Client,
    runtime: &mut AgentRuntime,
) -> Result<(), String> {
    let pull_url = format!(
        "{}/api/v1/client/devices/{}/commands/pull",
        runtime.cfg.base_url, runtime.cfg.device_id
    );
    let data = request_json(
        client,
        Method::GET,
        &pull_url,
        vec![("x-device-token", runtime.device_token.clone())],
        None,
    )
    .await?;

    let items = data
        .get("items")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    if !items.is_empty() {
        log::info!("client-control-agent pulled {} command(s)", items.len());
    }

    for item in items {
        let cmd_id = item
            .get("id")
            .and_then(|x| x.as_str())
            .map(str::trim)
            .unwrap_or_default()
            .to_owned();
        if cmd_id.is_empty() {
            continue;
        }
        let action = item
            .get("action")
            .and_then(|x| x.as_str())
            .map(str::trim)
            .unwrap_or_default()
            .to_owned();
        let payload = item.get("payload").cloned().unwrap_or_else(|| json!({}));
        let (ok, message, exec_payload) = execute_action(&action, &payload);

        let ack_url = format!(
            "{}/api/v1/client/devices/{}/commands/{}/ack",
            runtime.cfg.base_url, runtime.cfg.device_id, cmd_id
        );
        let ack_body = json!({
            "ok": ok,
            "message": message,
            "payload": {
                "action": action,
                "agent": "builtin-client-control-agent",
                "version": crate::VERSION,
                "result": exec_payload,
            }
        });

        request_json(
            client,
            Method::POST,
            &ack_url,
            vec![("x-device-token", runtime.device_token.clone())],
            Some(ack_body),
        )
        .await?;
    }

    Ok(())
}

fn execute_action(action: &str, payload: &Value) -> (bool, String, Value) {
    let normalized = action.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "ping" => (true, "pong".to_owned(), json!({"pong": true})),
        "set_option" | "set-option" => {
            let key = payload
                .get("key")
                .and_then(|x| x.as_str())
                .map(str::trim)
                .unwrap_or_default()
                .to_owned();
            if key.is_empty() {
                return (
                    false,
                    "set_option requires payload.key".to_owned(),
                    json!({}),
                );
            }
            let value = payload_value_to_string(payload.get("value"));
            LocalConfig::set_option(key.clone(), value.clone());
            (
                true,
                format!("set option {}", key),
                json!({
                    "key": key,
                    "value": value,
                }),
            )
        }
        "remove_option" | "remove-option" | "unset_option" | "unset-option" => {
            let key = payload
                .get("key")
                .and_then(|x| x.as_str())
                .map(str::trim)
                .unwrap_or_default()
                .to_owned();
            if key.is_empty() {
                return (
                    false,
                    "remove_option requires payload.key".to_owned(),
                    json!({}),
                );
            }
            LocalConfig::set_option(key.clone(), "".to_owned());
            (true, format!("removed option {}", key), json!({"key": key}))
        }
        "set_options" | "set-options" => {
            let obj = payload
                .get("options")
                .and_then(|x| x.as_object())
                .or_else(|| payload.as_object());
            let Some(obj) = obj else {
                return (
                    false,
                    "set_options requires payload object or payload.options object".to_owned(),
                    json!({}),
                );
            };
            let mut count = 0usize;
            for (k, v) in obj {
                if k.trim().is_empty() {
                    continue;
                }
                LocalConfig::set_option(k.clone(), payload_value_to_string(Some(v)));
                count += 1;
            }
            (
                true,
                format!("set {} option(s)", count),
                json!({ "count": count }),
            )
        }
        "set_permanent_password" | "set-permanent-password" => {
            let pwd = payload
                .get("password")
                .and_then(|x| x.as_str())
                .map(str::trim)
                .unwrap_or_default()
                .to_owned();
            if pwd.is_empty() {
                return (
                    false,
                    "set_permanent_password requires payload.password".to_owned(),
                    json!({}),
                );
            }
            Config::set_permanent_password(&pwd);
            (
                true,
                "permanent password updated".to_owned(),
                json!({"updated": true}),
            )
        }
        _ => (
            false,
            format!("unsupported action {}", action),
            json!({
                "unsupported_action": action,
            }),
        ),
    }
}

fn payload_value_to_string(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.to_owned(),
        Some(Value::Null) | None => "".to_owned(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => {
            if *b {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn unwrap_api_data(value: Value) -> Value {
    value.get("data").cloned().unwrap_or(value)
}

fn value_string(v: Option<&Value>) -> String {
    v.and_then(|x| x.as_str())
        .map(str::trim)
        .unwrap_or_default()
        .to_owned()
}

fn value_i64(v: Option<&Value>) -> i64 {
    v.and_then(|x| x.as_i64()).unwrap_or_default()
}

async fn select_service_node(
    nodes: &[Value],
    allow_manual_selection: bool,
    auto_select_mode: &str,
) -> Option<(Value, Option<i64>)> {
    if allow_manual_selection {
        if let Some(code) = read_env_trimmed(ENV_MANUAL_NODE_CODE)
            .or_else(|| read_option_trimmed(OPT_MANUAL_NODE_CODE))
            .filter(|x| !x.trim().is_empty())
        {
            if let Some(node) = nodes
                .iter()
                .find(|node| {
                    value_string(node.get("code")).eq_ignore_ascii_case(&code)
                        && is_manual_selectable(node)
                })
            {
                return Some((node.clone(), None));
            }
        }
    }

    if auto_select_prefers_latency(auto_select_mode) {
        if let Some(selected) = select_low_latency_service_node(nodes).await {
            return Some(selected);
        }
    }

    if let Some(code) = read_option_trimmed(OPT_SELECTED_NODE_CODE) {
        if let Some(node) = nodes
            .iter()
            .find(|node| value_string(node.get("code")).eq_ignore_ascii_case(&code))
        {
            if value_i64(node.get("healthScore")) >= 60 && is_candidate_selectable(node) {
                return Some((node.clone(), None));
            }
        }
    }

    nodes
        .iter()
        .max_by_key(|node| {
            let selectable_bonus = if node
                .get("manualSelectable")
                .and_then(|x| x.as_bool())
                .unwrap_or(true)
            {
                1_i64
            } else {
                0_i64
            };
            let health = value_i64(node.get("healthScore")).clamp(0, 100);
            let weight = value_i64(node.get("weight")).clamp(0, 100);
            (selectable_bonus, health * 1000 + weight)
        })
        .cloned()
        .map(|node| (node, None))
}

fn auto_select_prefers_latency(mode: &str) -> bool {
    let normalized = mode.trim().to_ascii_lowercase();
    if normalized.contains("latency") {
        return true;
    }
    mode.contains("自动择优") || mode.contains("latency")
}

fn is_manual_selectable(node: &Value) -> bool {
    node.get("manualSelectable")
        .and_then(|x| x.as_bool())
        .unwrap_or(true)
}

fn is_candidate_selectable(node: &Value) -> bool {
    is_manual_selectable(node) || value_i64(node.get("healthScore")) >= 60
}

async fn select_low_latency_service_node(nodes: &[Value]) -> Option<(Value, Option<i64>)> {
    let mut best: Option<(Value, i64)> = None;

    for node in nodes {
        let Some(latency_ms) = measure_node_latency(node).await else {
            continue;
        };
        match &best {
            Some((best_node, best_latency))
                if *best_latency < latency_ms
                    || (*best_latency == latency_ms
                        && rank_service_node(best_node) >= rank_service_node(node)) => {}
            _ => {
                best = Some((node.clone(), latency_ms));
            }
        }
    }

    best.map(|(node, latency_ms)| (node, Some(latency_ms)))
}

async fn measure_node_latency(node: &Value) -> Option<i64> {
    let target = service_node_probe_target(node);
    if target.is_empty() {
        return None;
    }
    let started = Instant::now();
    match connect_tcp(target.clone(), LATENCY_PROBE_TIMEOUT_MS).await {
        Ok(_) => Some(started.elapsed().as_millis() as i64),
        Err(err) => {
            log::debug!(
                "client-control-agent latency probe failed: node={}, target={}, err={}",
                value_string(node.get("code")),
                target,
                err
            );
            None
        }
    }
}

fn rank_service_node(node: &Value) -> i64 {
    let selectable_bonus = if node
        .get("manualSelectable")
        .and_then(|x| x.as_bool())
        .unwrap_or(true)
    {
        1_i64
    } else {
        0_i64
    };
    let health = value_i64(node.get("healthScore")).clamp(0, 100);
    let weight = value_i64(node.get("weight")).clamp(0, 100);
    selectable_bonus * 1_000_000 + health * 1000 + weight
}

fn service_node_probe_target(node: &Value) -> String {
    let probe_host = value_string(node.get("probeHost"));
    if !probe_host.is_empty() {
        return probe_host;
    }

    let relay = value_string(node.get("relayServer"));
    if !relay.is_empty() {
        if let Some((host, _port)) = relay.rsplit_once(':') {
            if !host.trim().is_empty() {
                return format!("{}:21116", host.trim());
            }
        }
        return relay;
    }

    value_string(node.get("idServer"))
}

fn persist_runtime_assignment_state(data: &Value) -> Result<(), String> {
    LocalConfig::set_option(
        OPT_RUNTIME_ASSIGNED_NODE_CODES.to_owned(),
        serde_json::to_string(
            &data
                .get("assignedNodeCodes")
                .cloned()
                .unwrap_or_else(|| Value::Array(vec![])),
        )
        .map_err(|err| format!("serialize assigned node codes failed: {}", err))?,
    );
    LocalConfig::set_option(
        OPT_RUNTIME_CLIENT_OPTIONS.to_owned(),
        serde_json::to_string(
            &data
                .get("clientOptions")
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
        .map_err(|err| format!("serialize client options failed: {}", err))?,
    );
    let fixed_password = value_string(data.get("fixedPassword"));
    LocalConfig::set_option(
        OPT_RUNTIME_FIXED_PASSWORD.to_owned(),
        fixed_password.clone(),
    );
    if !fixed_password.is_empty() {
        Config::set_permanent_password(&fixed_password);
    }
    LocalConfig::set_option(
        OPT_RUNTIME_DEVICE_ENABLED.to_owned(),
        if data
            .get("deviceEnabled")
            .and_then(|x| x.as_bool())
            .unwrap_or(true)
        {
            "Y".to_owned()
        } else {
            "N".to_owned()
        },
    );
    LocalConfig::set_option(
        OPT_RUNTIME_DEVICE_OWNER.to_owned(),
        value_string(data.get("deviceOwner")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_TENANT_ID.to_owned(),
        value_string(data.get("tenantId")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_TENANT_NAME.to_owned(),
        value_string(data.get("tenantName")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_TENANT_STATUS.to_owned(),
        value_string(data.get("tenantStatus")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_BILLING_STATE.to_owned(),
        value_string(data.get("billingState")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_SERVER_LOCK_MODE.to_owned(),
        value_string(data.get("serverLockMode")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_FIXED_ID_SERVER.to_owned(),
        value_string(data.get("fixedIdServer")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_FIXED_API_SERVER.to_owned(),
        value_string(data.get("fixedApiServer")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_FIXED_PUBLIC_KEY.to_owned(),
        value_string(data.get("fixedPublicKey")),
    );
    Ok(())
}

fn persist_runtime_service_nodes(
    data: &Value,
    service_nodes: &[Value],
    allow_manual_selection: bool,
) -> Result<(), String> {
    let raw = serde_json::to_string(service_nodes)
        .map_err(|err| format!("serialize runtime service nodes failed: {}", err))?;
    LocalConfig::set_option(OPT_RUNTIME_SERVICE_NODES.to_owned(), raw);
    LocalConfig::set_option(
        OPT_RUNTIME_AUTO_SELECT_MODE.to_owned(),
        value_string(data.get("autoSelectMode")),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_ALLOW_MANUAL_SELECTION.to_owned(),
        if allow_manual_selection {
            "Y".to_owned()
        } else {
            "N".to_owned()
        },
    );
    Ok(())
}

fn apply_runtime_service_node(
    data: &Value,
    node: &Value,
    selected_latency: Option<i64>,
) -> Result<(), String> {
    let code = value_string(node.get("code"));
    let probe_host = service_node_probe_target(node);
    let rendezvous = first_non_empty(&[
        value_string(data.get("fixedIdServer")),
        read_option_trimmed(OPT_RUNTIME_FIXED_ID_SERVER).unwrap_or_default(),
        value_string(node.get("idServer")),
    ]);
    if code.is_empty() || rendezvous.is_empty() {
        return Err("bootstrap selected node is missing code or fixedIdServer".to_owned());
    }

    let relay = value_string(node.get("relayServer"));
    let api = first_non_empty(&[
        value_string(data.get("fixedApiServer")),
        read_option_trimmed(OPT_RUNTIME_FIXED_API_SERVER).unwrap_or_default(),
        value_string(node.get("apiServer")),
    ]);
    let public_key = first_non_empty(&[
        value_string(data.get("fixedPublicKey")),
        read_option_trimmed(OPT_RUNTIME_FIXED_PUBLIC_KEY).unwrap_or_default(),
        value_string(node.get("publicKey")),
    ]);
    let expires_at = value_string(data.get("expiresAt"));
    let profile_name = value_string(data.get("profileName"));

    LocalConfig::set_option(OPT_SELECTED_NODE_CODE.to_owned(), code);
    LocalConfig::set_option(OPT_RUNTIME_SELECTED_NODE_PROBE_HOST.to_owned(), probe_host);
    LocalConfig::set_option(
        OPT_RUNTIME_SELECTED_NODE_RELAY_SERVER.to_owned(),
        relay.clone(),
    );
    LocalConfig::set_option(
        OPT_RUNTIME_SELECTED_NODE_API_SERVER.to_owned(),
        value_string(node.get("apiServer")),
    );
    LocalConfig::set_option(OPT_RUNTIME_RENDEZVOUS_SERVER.to_owned(), rendezvous);
    LocalConfig::set_option(OPT_RUNTIME_RELAY_SERVER.to_owned(), relay);
    LocalConfig::set_option(OPT_RUNTIME_API_SERVER.to_owned(), api);
    LocalConfig::set_option(OPT_RUNTIME_EXPIRES_AT.to_owned(), expires_at);
    LocalConfig::set_option(OPT_RUNTIME_PROFILE_NAME.to_owned(), profile_name);
    LocalConfig::set_option(
        OPT_RUNTIME_SELECTED_NODE_LATENCY.to_owned(),
        selected_latency.map(|x| x.to_string()).unwrap_or_default(),
    );
    Config::set_option(
        keys::OPTION_CUSTOM_RENDEZVOUS_SERVER.to_owned(),
        value_string(node.get("idServer")),
    );
    Config::set_option(
        keys::OPTION_RELAY_SERVER.to_owned(),
        value_string(node.get("relayServer")),
    );
    Config::set_option(
        keys::OPTION_API_SERVER.to_owned(),
        value_string(node.get("apiServer")),
    );
    if !public_key.is_empty() {
        Config::set_option(keys::OPTION_KEY.to_owned(), public_key);
    }

    Ok(())
}

fn first_non_empty(values: &[String]) -> String {
    values
        .iter()
        .find(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_default()
}

async fn request_json(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    headers: Vec<(&'static str, String)>,
    body: Option<Value>,
) -> Result<Value, String> {
    let mut req = client
        .request(method, url)
        .timeout(std::time::Duration::from_secs(12));
    for (k, v) in headers {
        if !v.trim().is_empty() {
            req = req.header(k, v);
        }
    }
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("request error: {:?}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("read body error: {:?}", e))?;
    let data = serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({ "raw": text }));
    if !status.is_success() {
        return Err(format!("http {}: {}", status, data));
    }
    Ok(data)
}

fn read_env_trimmed(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn read_option_trimmed(key: &str) -> Option<String> {
    let v = LocalConfig::get_option(key);
    let trimmed = v.trim();
    if trimmed.is_empty() {
        let v2 = Config::get_option(key);
        let t2 = v2.trim();
        if t2.is_empty() {
            None
        } else {
            Some(t2.to_owned())
        }
    } else {
        Some(trimmed.to_owned())
    }
}

fn read_u64(env_key: &str, opt_key: &str, default_v: u64) -> u64 {
    if let Some(v) = read_env_trimmed(env_key) {
        if let Ok(x) = v.parse::<u64>() {
            return x;
        }
    }
    if let Some(v) = read_option_trimmed(opt_key) {
        if let Ok(x) = v.parse::<u64>() {
            return x;
        }
    }
    default_v
}

fn runtime_device_uuid() -> String {
    crate::encode64(hbb_common::get_uuid())
}

fn normalize_base_url(raw: &str) -> String {
    let x = raw.trim().trim_end_matches('/');
    if x.starts_with("http://") || x.starts_with("https://") {
        x.to_owned()
    } else {
        String::new()
    }
}

fn derive_control_base_from_runtime() -> Option<String> {
    let api_server = crate::common::get_api_server(
        Config::get_option("api-server"),
        Config::get_option("custom-rendezvous-server"),
    );
    if api_server.is_empty() || crate::common::is_public(&api_server) {
        return None;
    }
    let normalized = normalize_base_url(&api_server);
    if normalized.is_empty() {
        return None;
    }
    if let Ok(mut u) = url::Url::parse(&normalized) {
        if matches!(u.port(), Some(21114)) {
            let _ = u.set_port(Some(22118));
        }
        u.set_path("");
        u.set_query(None);
        u.set_fragment(None);
        return Some(u.to_string().trim_end_matches('/').to_owned());
    }
    None
}

fn is_auth_error(err: &str) -> bool {
    err.contains("401") || err.contains("403") || err.to_ascii_lowercase().contains("unauthorized")
}
