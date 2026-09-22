//! Seek pages reread metadata/deletions within a fixed upper key; later in-range inserts may appear.

use std::collections::BinaryHeap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ws_ckpt_common::{
    encoded_payload_size, ErrorCode, Response, SnapshotEntry, SnapshotSummary, SnapshotSummaryMeta,
    LIST_PAGE_TARGET_BYTES, MAX_FRAME_SIZE, MAX_LIST_CURSOR_BYTES, MAX_LIST_PAGE_LIMIT,
};

use crate::state::DaemonState;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct Key {
    created_at: DateTime<Utc>,
    ws_id: String,
    id: String,
}

impl Key {
    fn parts(&self) -> (DateTime<Utc>, &str, &str) {
        (self.created_at, &self.ws_id, &self.id)
    }

    fn valid(&self) -> bool {
        // These identifiers are only compared or used in in-memory map lookups.
        // Do not impose V2 path-component rules on legacy snapshot identifiers.
        !self.ws_id.is_empty() && !self.id.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    scope: Option<String>,
    after: Key,
    upper: Key,
}

impl Cursor {
    fn decode(value: &str, scope: Option<&str>) -> Option<Self> {
        if value.len() > MAX_LIST_CURSOR_BYTES {
            return None;
        }
        let bytes = hex::decode(value).ok()?;
        let cursor: Self = serde_json::from_slice(&bytes).ok()?;
        if cursor.version != 1
            || cursor.scope.as_deref() != scope
            || !cursor.after.valid()
            || !cursor.upper.valid()
            || cursor.after >= cursor.upper
            || scope.is_some_and(|id| {
                id.is_empty() || cursor.after.ws_id != id || cursor.upper.ws_id != id
            })
        {
            return None;
        }
        Some(cursor)
    }
}

struct Candidates {
    keys: Vec<Key>,
    upper: Option<Key>,
    has_more: bool,
}

/// Return an ascending keyset page without changing the legacy List endpoint.
///
/// A single entry may exceed the target budget, but never the transport frame.
/// No workspace guards or cursor sessions survive this request.
pub(crate) async fn list_page(
    state: &Arc<DaemonState>,
    workspace: Option<&str>,
    limit: u32,
    cursor: Option<&str>,
) -> anyhow::Result<Response> {
    if limit == 0 || limit > MAX_LIST_PAGE_LIMIT {
        return Ok(Response::Error {
            code: ErrorCode::InvalidListRequest,
            message: format!("list page limit must be between 1 and {MAX_LIST_PAGE_LIMIT}"),
        });
    }

    let scope = if let Some(workspace) = workspace {
        let Some(arc) = state.resolve_workspace(workspace).await else {
            return Ok(Response::Error {
                code: ErrorCode::WorkspaceNotFound,
                message: format!("workspace not found: {workspace}"),
            });
        };
        if let Some(response) = state.detached_registration_error(&arc).await {
            return Ok(response);
        }
        let id = arc.read().await.ws_id.clone();
        Some(id)
    } else {
        None
    };
    let cursor = match cursor {
        Some(value) => match Cursor::decode(value, scope.as_deref()) {
            Some(cursor) => Some(cursor),
            None => {
                return Ok(Response::Error {
                    code: ErrorCode::InvalidListCursor,
                    message: "invalid list cursor or workspace scope mismatch".to_string(),
                });
            }
        },
        None => None,
    };
    let after = cursor.as_ref().map(|cursor| cursor.after.clone());
    let candidates = select_candidates(
        state,
        scope.as_deref(),
        after.as_ref(),
        cursor.as_ref().map(|cursor| &cursor.upper),
        limit as usize + 1,
    )
    .await;
    collect_page(state, scope, limit as usize, after, candidates).await
}

async fn select_candidates(
    state: &DaemonState,
    scope: Option<&str>,
    after: Option<&Key>,
    upper: Option<&Key>,
    capacity: usize,
) -> Candidates {
    let workspaces = match scope {
        Some(ws_id) => state.get_by_wsid(ws_id).into_iter().collect(),
        None => state.all_workspaces(),
    };
    let mut heap = BinaryHeap::<Key>::with_capacity(capacity);
    let mut maximum = upper.cloned();
    let mut has_more = false;
    for arc in workspaces {
        let ws = arc.read().await;
        if !state.workspace_arc_is_current(&ws.ws_id, &arc) {
            continue;
        }
        // Every page rescans the HashMap; bounded keys do not bound CPU or read-lock duration.
        for (visited, (id, meta)) in ws.index.snapshots.iter().enumerate() {
            if visited != 0 && visited % 4096 == 0 {
                // Yield executor time, retaining the guard so concurrent writes cannot invalidate iteration.
                tokio::task::yield_now().await;
            }
            let parts = (meta.created_at, ws.ws_id.as_str(), id.as_str());
            let key = || Key {
                created_at: meta.created_at,
                ws_id: ws.ws_id.clone(),
                id: id.clone(),
            };
            // Discover the first request's upper bound in this same scan. Only
            // retained candidates and a changing maximum allocate key strings.
            if upper.is_none() && maximum.as_ref().is_none_or(|max| parts > max.parts()) {
                maximum = Some(key());
            }
            if after.is_some_and(|after| parts <= after.parts())
                || upper.is_some_and(|upper| parts > upper.parts())
            {
                continue;
            }
            if heap.len() < capacity {
                heap.push(key());
            } else {
                has_more = true;
                if let Some(mut largest) = heap.peek_mut() {
                    if parts < largest.parts() {
                        *largest = key();
                    }
                }
            }
        }
        drop(ws);
        tokio::task::yield_now().await;
    }
    Candidates {
        keys: heap.into_sorted_vec(),
        upper: maximum,
        has_more,
    }
}

async fn collect_page(
    state: &DaemonState,
    scope: Option<String>,
    limit: usize,
    mut after: Option<Key>,
    mut candidates: Candidates,
) -> anyhow::Result<Response> {
    let Some(upper) = candidates.upper.take() else {
        return Ok(page(Vec::new(), None));
    };
    let mut snapshots = Vec::new();
    let mut entries_bytes = 0;
    let mut next_cursor = None;
    loop {
        let mut keys = candidates.keys.into_iter().peekable();
        let mut invalidated = false;
        while let Some(key) = keys.next() {
            // Even vanished or changed-key candidates advance the internal scan.
            // The externally returned cursor stays at the last returned entry,
            // whose complete envelope was budgeted before cloning its metadata.
            after = Some(key.clone());
            let Some(arc) = state.get_by_wsid(&key.ws_id) else {
                invalidated = true;
                continue;
            };
            let ws = arc.read().await;
            let Some(meta) = ws.index.snapshots.get(&key.id).filter(|meta| {
                meta.created_at == key.created_at
                    && ws.ws_id == key.ws_id
                    && state.workspace_arc_is_current(&key.ws_id, &arc)
            }) else {
                invalidated = true;
                continue;
            };
            if snapshots.len() == limit {
                return Ok(page(snapshots, next_cursor));
            }

            let more = keys.peek().is_some() || candidates.has_more || (invalidated && key < upper);
            let candidate_cursor = if more {
                let token = hex::encode(serde_json::to_vec(&Cursor {
                    version: 1,
                    scope: scope.clone(),
                    after: key.clone(),
                    upper: upper.clone(),
                })?);
                if token.len() > MAX_LIST_CURSOR_BYTES {
                    if !snapshots.is_empty() {
                        return Ok(page(snapshots, next_cursor));
                    }
                    return Ok(entry_too_large(&key));
                }
                Some(token)
            } else {
                None
            };
            let workspace = ws.index.workspace_path.to_string_lossy();
            // Structs and tuples encode fields in the same fixed-int order.
            // Borrow SnapshotMeta to retain its metadata serializer without
            // cloning a potentially unframeable entry (see equivalence tests).
            let entry_bytes = encoded_payload_size(&(&key.id, workspace.as_ref(), meta))?;
            let total = entries_bytes
                + entry_bytes
                + encoded_payload_size(&page(Vec::new(), candidate_cursor.clone()))?;
            if !snapshots.is_empty() && total > LIST_PAGE_TARGET_BYTES {
                return Ok(page(snapshots, next_cursor));
            }
            if total > u64::from(MAX_FRAME_SIZE) {
                let summary_meta = SnapshotSummaryMeta {
                    pinned: meta.pinned,
                    created_at: meta.created_at,
                    missing: meta.missing,
                };
                let envelope = Response::ListPageSummaryOk {
                    snapshot: SnapshotSummary {
                        id: String::new(),
                        workspace: String::new(),
                        meta: summary_meta,
                    },
                    next_cursor: candidate_cursor.clone(),
                };
                let summary_bytes =
                    encoded_payload_size(&envelope)? + key.id.len() as u64 + workspace.len() as u64;
                if summary_bytes > u64::from(MAX_FRAME_SIZE) {
                    return Ok(entry_too_large(&key));
                }
                return Ok(Response::ListPageSummaryOk {
                    snapshot: SnapshotSummary {
                        id: key.id,
                        workspace: workspace.into_owned(),
                        meta: summary_meta,
                    },
                    next_cursor: candidate_cursor,
                });
            }
            snapshots.push(SnapshotEntry {
                id: key.id,
                workspace: workspace.into_owned(),
                meta: meta.clone(),
            });
            entries_bytes += entry_bytes;
            next_cursor = candidate_cursor;
        }
        if !candidates.has_more && !invalidated {
            return Ok(page(snapshots, None));
        }
        // Deletions can consume the entire lookahead batch. Refill rather than
        // returning an empty continuation or mistaking missing keys for the end.
        candidates = select_candidates(
            state,
            scope.as_deref(),
            after.as_ref(),
            Some(&upper),
            limit - snapshots.len() + 1,
        )
        .await;
        if candidates.keys.is_empty() {
            return Ok(page(snapshots, None));
        }
    }
}

fn page(snapshots: Vec<SnapshotEntry>, next_cursor: Option<String>) -> Response {
    Response::ListPageOk {
        snapshots,
        next_cursor,
    }
}

fn entry_too_large(key: &Key) -> Response {
    Response::Error {
        code: ErrorCode::ListEntryTooLarge,
        message: format!(
            "snapshot identity/basic fields or its required cursor exceed the list budgets even without metadata (snapshot ID prefix {:?}, workspace ID prefix {:?}); no entry was skipped",
            key.id.chars().take(128).collect::<String>(),
            key.ws_id.chars().take(128).collect::<String>()
        ),
    }
}

#[cfg(test)]
mod tests;
