use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::state::DaemonState;
use ws_ckpt_common::{decode_payload, encode_frame, ErrorCode, Request, Response, MAX_FRAME_SIZE};

pub async fn run_listener(
    state: Arc<DaemonState>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // 1. Clean up residual socket file
    let _ = std::fs::remove_file(&state.socket_path);

    // 2. Ensure socket parent directory exists
    if let Some(parent) = state.socket_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("Failed to create socket parent directory")?;
    }

    // 3. Bind the Unix listener
    let listener = UnixListener::bind(&state.socket_path).context("Failed to bind Unix socket")?;
    info!("Listening on {:?}", state.socket_path);

    // 4. Set socket permissions to 0o666
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&state.socket_path, std::fs::Permissions::from_mode(0o666))
        .context("Failed to set socket permissions")?;

    // 5. Accept loop
    let mut join_set = JoinSet::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        let state = Arc::clone(&state);
                        join_set.spawn(async move {
                            if let Err(e) = handle_connection(stream, state).await {
                                error!("Connection error: {:#}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
            _ = cancel.cancelled() => {
                info!("Listener received cancellation signal");
                break;
            }
        }

        // Reap finished connections. JoinSet holds each task's handle and output
        // slot until it is joined, so without this the set grows by one entry per
        // CLI invocation and is only released at shutdown. try_join_next never
        // awaits, so reaping cannot delay the next accept; a join_next branch in
        // the select! above would instead need a second mutable borrow of
        // join_set while the accept arm still spawns into it.
        while join_set.try_join_next().is_some() {}
    }

    // 7. Wait for in-flight tasks to complete (with timeout)
    info!("Waiting for in-flight connections to complete...");
    let drain = async { while join_set.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .is_err()
    {
        error!("Timed out waiting for in-flight connections; aborting remaining tasks");
        join_set.abort_all();
    }

    // Clean up socket file
    let _ = std::fs::remove_file(&state.socket_path);
    info!("Listener shut down");
    Ok(())
}

async fn handle_connection(
    mut stream: tokio::net::UnixStream,
    state: Arc<DaemonState>,
) -> anyhow::Result<()> {
    // Read 4-byte LE length
    let len = stream
        .read_u32_le()
        .await
        .context("Failed to read frame length")?;

    // Validate frame size
    if len > MAX_FRAME_SIZE {
        let err_resp = Response::Error {
            code: ErrorCode::InternalError,
            message: format!("Frame too large: {} bytes (max {})", len, MAX_FRAME_SIZE),
        };
        let frame = encode_frame(&err_resp)?;
        stream.write_all(&frame).await?;
        anyhow::bail!("Frame too large: {} bytes", len);
    }

    // Read payload
    let mut payload = vec![0u8; len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .context("Failed to read frame payload")?;

    // Decode request
    let request: Request = decode_payload(&payload).context("Failed to decode request")?;

    let peer_cred = stream.peer_cred().ok();
    let agent_name = peer_cred
        .as_ref()
        .and_then(|cred| cred.pid().map(|p| p as u32))
        .map(crate::ops_log::detect_agent_name)
        .unwrap_or_else(|| "user".to_string());
    let ops_name = crate::ops_log::ops_name_from_request(&request);

    // Dispatch
    let context = crate::dispatcher::DispatchContext::new(peer_cred.map(|cred| cred.uid()));
    let mut response = crate::dispatcher::dispatch_with_context(&state, request, context).await;
    let frame = encode_response(&mut response)?;

    if let Some(name) = ops_name {
        crate::ops_log::log_operation(name, &agent_name, &response);
    }

    stream
        .write_all(&frame)
        .await
        .context("Failed to write response")?;

    Ok(())
}

fn encode_response(response: &mut Response) -> anyhow::Result<Vec<u8>> {
    match encode_frame(response) {
        Ok(frame) => Ok(frame),
        Err(error) => {
            let advice = if matches!(response, Response::ListOk { .. }) {
                "Use paginated listing or request snapshot counts."
            } else {
                "The request may already have completed; check its outcome before retrying."
            };
            *response = Response::Error {
                // Legacy clients cannot decode newly appended error-code variants.
                code: ErrorCode::InternalError,
                message: format!("Failed to encode response: {error}. {advice}"),
            };
            encode_frame(response).context("Failed to encode error response")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::btrfs_loop::BtrfsLoopBackend;
    use ws_ckpt_common::{DaemonConfig, SnapshotIndex, SnapshotMeta, DEFAULT_LIST_PAGE_LIMIT};

    fn fixture(count: usize, message_bytes: usize) -> (tempfile::TempDir, Arc<DaemonState>) {
        let dir = tempfile::tempdir().unwrap();
        let mount = dir.path().join("backend");
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let state = Arc::new(DaemonState::new(
            DaemonConfig {
                mount_path: mount.clone(),
                ..DaemonConfig::default()
            },
            Arc::new(BtrfsLoopBackend::new(mount, dir.path().join("image"))),
            dir.path().join("state"),
        ));
        let mut index = SnapshotIndex::new(workspace.clone());
        let now = chrono::Utc::now();
        for i in 0..count {
            index.snapshots.insert(
                format!("snap-{i:04}"),
                SnapshotMeta {
                    message: Some("x".repeat(message_bytes)),
                    metadata: None,
                    pinned: false,
                    created_at: now,
                    missing: false,
                    parent_id: None,
                    child_ids: vec![],
                },
            );
        }
        state
            .register_workspace("ws-test".into(), workspace, index)
            .unwrap();
        (dir, state)
    }

    async fn exchange(state: Arc<DaemonState>, request: Request) -> Response {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("listener.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, state).await.unwrap();
        });
        let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
        client
            .write_all(&encode_frame(&request).unwrap())
            .await
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), async {
            let size = client.read_u32_le().await.unwrap();
            assert!(size <= MAX_FRAME_SIZE);
            let mut payload = vec![0; size as usize];
            client.read_exact(&mut payload).await.unwrap();
            decode_payload(&payload).unwrap()
        })
        .await
        .unwrap();
        server.await.unwrap();
        response
    }

    #[tokio::test]
    async fn oversized_legacy_list_errors_but_pages_and_other_requests_work() {
        let (_dir, state) = fixture(18, 1024 * 1024);
        let response = exchange(
            state.clone(),
            Request::List {
                workspace: None,
                format: None,
            },
        )
        .await;
        match response {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::InternalError);
                assert!(message.contains("paginated listing"));
                assert!(message.contains("snapshot counts"));
                assert!(!message.contains("ws-ckpt list"));
            }
            other => panic!("expected legacy-compatible error, got {other:?}"),
        }
        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let response = exchange(
                state.clone(),
                Request::ListPage {
                    workspace: None,
                    limit: DEFAULT_LIST_PAGE_LIMIT,
                    cursor: cursor.clone(),
                },
            )
            .await;
            match response {
                Response::ListPageOk {
                    snapshots,
                    next_cursor,
                } => {
                    assert_eq!(snapshots.len(), 1);
                    ids.extend(snapshots.into_iter().map(|entry| entry.id));
                    if next_cursor.is_none() {
                        break;
                    }
                    assert_ne!(next_cursor, cursor);
                    assert!(ids.len() < 18);
                    cursor = next_cursor;
                }
                other => panic!("expected page, got {other:?}"),
            }
        }
        assert_eq!(
            ids,
            (0..18).map(|i| format!("snap-{i:04}")).collect::<Vec<_>>()
        );
        assert!(matches!(
            exchange(state, Request::Config).await,
            Response::ConfigOk { .. }
        ));
    }

    #[tokio::test]
    async fn oversized_entries_return_summaries_and_continue_over_socket() {
        let (_dir, state) = fixture(3, MAX_FRAME_SIZE as usize);
        let mut cursor = None;
        for i in 0..3 {
            let response = exchange(
                state.clone(),
                Request::ListPage {
                    workspace: None,
                    limit: 100,
                    cursor: cursor.clone(),
                },
            )
            .await;
            let Response::ListPageSummaryOk {
                snapshot,
                next_cursor,
            } = response
            else {
                panic!("expected a summary page");
            };
            assert_eq!(snapshot.id, format!("snap-{i:04}"));
            assert_eq!(next_cursor.is_none(), i == 2);
            assert_ne!(cursor, next_cursor);
            cursor = next_cursor;
        }
        assert!(matches!(
            exchange(state, Request::Config).await,
            Response::ConfigOk { .. }
        ));
    }

    #[test]
    fn oversized_non_list_responses_return_legacy_compatible_errors() {
        use ws_ckpt_common::{ChangeType, DiffEntry, StatusReport, WorkspaceInfo};

        for mut response in [
            Response::DiffOk {
                changes: vec![DiffEntry {
                    path: "file".into(),
                    change_type: ChangeType::Modified,
                    detail: Some("x".repeat(MAX_FRAME_SIZE as usize)),
                }],
            },
            Response::StatusOk {
                report: StatusReport {
                    uptime_secs: 0,
                    workspaces: vec![WorkspaceInfo {
                        ws_id: "ws-test".into(),
                        path: "x".repeat(MAX_FRAME_SIZE as usize),
                        snapshot_count: 0,
                    }],
                    fs_total_bytes: 0,
                    fs_used_bytes: 0,
                },
            },
            Response::Error {
                code: ErrorCode::InternalError,
                message: "x".repeat(MAX_FRAME_SIZE as usize),
            },
        ] {
            let frame = encode_response(&mut response).unwrap();
            assert!(frame.len() < 1024);
            assert_eq!(u32::from_le_bytes(frame[4..8].try_into().unwrap()), 4);
            assert_eq!(u32::from_le_bytes(frame[8..12].try_into().unwrap()), 7);
            match decode_payload::<Response>(&frame[4..]).unwrap() {
                Response::Error { code, message } => {
                    assert_eq!(code, ErrorCode::InternalError);
                    assert!(message.contains("frame too large"));
                    assert!(message.contains(&MAX_FRAME_SIZE.to_string()));
                    assert!(message.contains("may already have completed"));
                }
                other => panic!("expected structured error, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn small_legacy_list_retains_its_response_variant() {
        let (_dir, state) = fixture(2, 16);
        let response = exchange(
            state,
            Request::List {
                workspace: None,
                format: None,
            },
        )
        .await;
        assert!(matches!(response, Response::ListOk { snapshots } if snapshots.len() == 2));
    }
}
