use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use ws_ckpt_common::{Request, Response};

const OPS_LOG_PATH: &str = "/var/log/anolisa/sls/ops/ws-ckpt.jsonl";
const TELEMETRY_GATE: &str = "/etc/anolisa/.telemetry_disabled";
const KNOWN_AGENTS: &[&str] = &["user", "hermes", "openclaw"];
static OPS_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize)]
struct OpsRecord<'a> {
    #[serde(rename = "component.name")]
    component_name: &'static str,
    #[serde(rename = "component.version")]
    component_version: &'static str,
    #[serde(rename = "component.agent_name")]
    component_agent_name: &'a str,
    ops_id: String,
    ops_name: &'static str,
    ckpt_time: u32,
    roll_time: u32,
    diff_time: u32,
    list_time: u32,
    ops_time: u32,
    // Omit page completion when unknown or inapplicable to preserve legacy records.
    #[serde(skip_serializing_if = "Option::is_none")]
    list_page_last: Option<bool>,
    err_reason: &'a str,
    supply: &'a str,
}

pub fn ops_name_from_request(req: &Request) -> Option<&'static str> {
    match req {
        Request::Checkpoint { .. } | Request::GuardedCheckpointV2 { .. } => Some("ckpt"),
        Request::Rollback { .. } => Some("roll"),
        Request::Diff { .. } => Some("diff"),
        Request::List { .. } => Some("list"),
        Request::ListPage { .. } => Some("list_page"),
        Request::Config
        | Request::ReloadConfig
        | Request::ReloadGlobalConfig
        | Request::ConfigOverview
        | Request::GetWorkspacePolicy { .. }
        | Request::ResetWorkspacePolicy { .. }
        | Request::PatchWorkspacePolicy { .. }
        | Request::ReloadWorkspacePolicy { .. } => Some("config"),
        _ => None,
    }
}

/// Read `WS_CKPT_AGENT_NAME` from `/proc/{pid}/environ`.
/// Env unset → `"user"` (direct CLI). Env set but not in whitelist → `"unknown"`.
pub fn detect_agent_name(pid: u32) -> String {
    let path = format!("/proc/{pid}/environ");
    let Ok(data) = std::fs::read(&path) else {
        return "user".to_string();
    };
    for entry in data.split(|&b| b == 0) {
        if let Some(val) = entry.strip_prefix(b"WS_CKPT_AGENT_NAME=") {
            return match std::str::from_utf8(val) {
                Ok(s) if KNOWN_AGENTS.contains(&s) => s.to_string(),
                _ => "unknown".to_string(),
            };
        }
    }
    "user".to_string()
}

pub fn log_operation(ops_name: &'static str, agent_name: &str, response: &Response) {
    if std::path::Path::new(TELEMETRY_GATE).exists() || !std::path::Path::new(OPS_LOG_PATH).exists()
    {
        return;
    }

    let ops_id = format!(
        "{}-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        std::process::id(),
        OPS_SEQ.fetch_add(1, Ordering::Relaxed),
    );
    let record = build_record(ops_name, agent_name, response, ops_id);

    if let Err(e) = write_record(&record) {
        tracing::debug!("ops log write failed: {e}");
    }
}

fn build_record<'a>(
    ops_name: &'static str,
    agent_name: &'a str,
    response: &'a Response,
    ops_id: String,
) -> OpsRecord<'a> {
    let err_reason = match response {
        Response::Error { message, .. } | Response::GuardedCheckpointV2Rejected { message, .. } => {
            message.as_str()
        }
        _ => "none",
    };
    let list_page_last = match (ops_name, response) {
        (
            "list_page",
            Response::ListPageOk { next_cursor, .. }
            | Response::ListPageSummaryOk { next_cursor, .. },
        ) => Some(next_cursor.is_none()),
        _ => None,
    };

    OpsRecord {
        component_name: "ws-ckpt",
        component_version: env!("CARGO_PKG_VERSION"),
        component_agent_name: agent_name,
        ops_id,
        ops_name,
        ckpt_time: u32::from(ops_name == "ckpt"),
        roll_time: u32::from(ops_name == "roll"),
        diff_time: u32::from(ops_name == "diff"),
        list_time: u32::from(matches!(ops_name, "list" | "list_page")),
        ops_time: 1,
        list_page_last,
        err_reason,
        supply: "none",
    }
}

fn write_record(record: &OpsRecord<'_>) -> std::io::Result<()> {
    let mut line = serde_json::to_string(record)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    line.push('\n');

    let mut file = OpenOptions::new().append(true).open(OPS_LOG_PATH)?;
    file.write_all(line.as_bytes())?;
    file.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ops_name_mapping() {
        let ckpt = Request::Checkpoint {
            workspace: "w".into(),
            id: "id".into(),
            message: None,
            metadata: None,
            pin: false,
        };
        assert_eq!(ops_name_from_request(&ckpt), Some("ckpt"));

        let roll = Request::Rollback {
            workspace: "w".into(),
            to: None,
            num_ancestors: None,
        };
        assert_eq!(ops_name_from_request(&roll), Some("roll"));

        let diff = Request::Diff {
            workspace: "w".into(),
            from: "a".into(),
            to: None,
        };
        assert_eq!(ops_name_from_request(&diff), Some("diff"));

        let list = Request::List {
            workspace: Some("w".into()),
            format: None,
        };
        assert_eq!(ops_name_from_request(&list), Some("list"));

        for cursor in [None, Some("continuation".to_string())] {
            let page = Request::ListPage {
                workspace: Some("w".into()),
                limit: 1,
                cursor,
            };
            assert_eq!(ops_name_from_request(&page), Some("list_page"));
        }

        assert_eq!(ops_name_from_request(&Request::Config), Some("config"));
        assert_eq!(
            ops_name_from_request(&Request::ReloadConfig),
            Some("config")
        );
        assert_eq!(
            ops_name_from_request(&Request::ConfigOverview),
            Some("config")
        );

        let init = Request::Init {
            workspace: "w".into(),
        };
        assert_eq!(ops_name_from_request(&init), None);

        assert_eq!(ops_name_from_request(&Request::HealthAdvisory), None);

        let guarded = Request::GuardedCheckpointV2 {
            ws_id: "ws-abcdef".into(),
            expected_generation: ws_ckpt_common::WorkspaceGenerationTokenV2::from_bytes([1; 32]),
            checkpoint_id: "checkpoint-1".into(),
            operation_digest: [2; 32],
            message: None,
            metadata: None,
            pin: false,
        };
        assert_eq!(ops_name_from_request(&guarded), Some("ckpt"));

        let identity = Request::WorkspaceIdentityV2 {
            registration_path: "/workspace".into(),
        };
        assert_eq!(ops_name_from_request(&identity), None);

        let evidence = Request::CheckpointEvidenceV2 {
            ws_id: "ws-abcdef".into(),
            expected_generation: ws_ckpt_common::WorkspaceGenerationTokenV2::from_bytes([1; 32]),
            checkpoint_id: "checkpoint-1".into(),
            operation_digest: [2; 32],
        };
        assert_eq!(ops_name_from_request(&evidence), None);
    }

    #[test]
    fn record_serialization() {
        for (ops_name, response, ckpt_time, list_time) in [
            (
                "ckpt",
                Response::CheckpointOk {
                    snapshot_id: "id".into(),
                },
                1,
                0,
            ),
            ("list", Response::ListOk { snapshots: vec![] }, 0, 1),
        ] {
            let record = build_record(
                ops_name,
                "user",
                &response,
                "1719100800000-1234".to_string(),
            );
            let parsed = serde_json::to_value(&record).expect("serialize");

            assert_eq!(
                parsed,
                serde_json::json!({
                    "component.name": "ws-ckpt",
                    "component.version": env!("CARGO_PKG_VERSION"),
                    "component.agent_name": "user",
                    "ops_id": "1719100800000-1234",
                    "ops_name": ops_name,
                    "ckpt_time": ckpt_time,
                    "roll_time": 0,
                    "diff_time": 0,
                    "list_time": list_time,
                    "ops_time": 1,
                    "err_reason": "none",
                    "supply": "none",
                })
            );
        }
    }

    #[test]
    fn list_page_records_mark_terminal_and_nonterminal_replies() {
        for (next_cursor, is_last) in [(None, true), (Some("continuation".to_string()), false)] {
            let response = Response::ListPageOk {
                snapshots: vec![],
                next_cursor,
            };
            let record = build_record("list_page", "user", &response, "test-id".into());
            assert_eq!(record.list_page_last, Some(is_last));
            let parsed = serde_json::to_value(&record).expect("serialize");

            assert_eq!(parsed["ops_name"], "list_page");
            assert_eq!(parsed["list_page_last"], is_last);
            assert_eq!(parsed["list_time"], 1);
            assert_eq!(parsed["ops_time"], 1);
            assert_eq!(parsed["ckpt_time"], 0);
            assert_eq!(parsed["roll_time"], 0);
            assert_eq!(parsed["diff_time"], 0);
            assert_eq!(parsed["err_reason"], "none");
        }
    }

    #[test]
    fn summary_pages_keep_page_counters_and_terminal_flags() {
        for next_cursor in [None, Some("next".to_string())] {
            let response = Response::ListPageSummaryOk {
                snapshot: ws_ckpt_common::SnapshotSummary {
                    id: "snapshot".into(),
                    workspace: "/ws".into(),
                    meta: ws_ckpt_common::SnapshotSummaryMeta {
                        pinned: true,
                        created_at: chrono::Utc::now(),
                        missing: true,
                    },
                },
                next_cursor: next_cursor.clone(),
            };
            let record = build_record("list_page", "user", &response, "test-id".into());
            assert_eq!(record.list_page_last, Some(next_cursor.is_none()));
            assert_eq!(record.list_time, 1);
            assert_eq!(record.ops_time, 1);
            assert_eq!(record.err_reason, "none");
            assert!(build_record("list", "user", &response, "test-id".into())
                .list_page_last
                .is_none());
        }
    }

    #[test]
    fn list_page_errors_keep_the_operation_without_a_last_flag() {
        let response = Response::Error {
            code: ws_ckpt_common::ErrorCode::WorkspaceNotFound,
            message: "workspace not found".into(),
        };
        for cursor in [None, Some("continuation".to_string())] {
            let request = Request::ListPage {
                workspace: Some("w".into()),
                limit: 1,
                cursor,
            };
            let ops_name = ops_name_from_request(&request).expect("page operation");
            let record = build_record(ops_name, "hermes", &response, "test-id".into());
            assert_eq!(record.list_page_last, None);
            let parsed = serde_json::to_value(&record).expect("serialize");

            assert_eq!(parsed["ops_name"], "list_page");
            assert_eq!(parsed["list_time"], 1);
            assert_eq!(parsed["ops_time"], 1);
            assert_eq!(parsed["err_reason"], "workspace not found");
            assert!(parsed.get("list_page_last").is_none());
        }
    }

    #[test]
    fn last_flag_requires_both_page_operation_and_page_reply() {
        for (ops_name, response) in [
            (
                "list",
                Response::ListPageOk {
                    snapshots: vec![],
                    next_cursor: None,
                },
            ),
            ("list_page", Response::ListOk { snapshots: vec![] }),
        ] {
            let record = build_record(ops_name, "user", &response, "test-id".into());
            assert_eq!(record.list_page_last, None);
            let parsed = serde_json::to_value(&record).expect("serialize");
            assert!(parsed.get("list_page_last").is_none());
        }
    }

    #[test]
    fn record_with_error() {
        let response = Response::Error {
            code: ws_ckpt_common::ErrorCode::SnapshotNotFound,
            message: "snapshot not found".into(),
        };
        let record = build_record(
            "roll",
            "hermes",
            &response,
            "1719100800000-1234".to_string(),
        );

        let json = serde_json::to_string(&record).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse");

        assert_eq!(parsed["component.agent_name"], "hermes");
        assert_eq!(parsed["ops_name"], "roll");
        assert_eq!(parsed["roll_time"], 1);
        assert_eq!(parsed["ckpt_time"], 0);
        assert_eq!(parsed["err_reason"], "snapshot not found");
    }

    #[test]
    fn write_to_tempfile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ws-ckpt.jsonl");
        std::fs::File::create(&path).expect("create");

        let response = Response::ListOk { snapshots: vec![] };
        let record = build_record("list", "user", &response, "test-id".into());

        let mut line = serde_json::to_string(&record).expect("serialize");
        line.push('\n');
        let mut file = OpenOptions::new().append(true).open(&path).expect("open");
        file.write_all(line.as_bytes()).expect("write");
        file.flush().expect("flush");

        let contents = std::fs::read_to_string(&path).expect("read");
        assert_eq!(contents.lines().count(), 1);
        let parsed: serde_json::Value =
            serde_json::from_str(contents.lines().next().unwrap()).expect("parse");
        assert_eq!(parsed["ops_name"], "list");
        assert_eq!(parsed["list_time"], 1);
    }
}
