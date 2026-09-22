use super::*;
use std::path::PathBuf;
use ws_ckpt_common::{
    encode_frame, DaemonConfig, Request, SnapshotIndex, SnapshotMeta, DEFAULT_LIST_PAGE_LIMIT,
};

struct Fixture {
    root: tempfile::TempDir,
    state: Arc<DaemonState>,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        // Only path accessors are used; no backend bootstrap or btrfs operation.
        let backend = Arc::new(crate::backends::btrfs_loop::BtrfsLoopBackend::new(
            root.path().join("mount"),
            root.path().join("test.img"),
        ));
        let state = Arc::new(DaemonState::new(
            DaemonConfig::default(),
            backend,
            root.path().join("state"),
        ));
        Self { root, state }
    }

    fn add_workspace(
        &self,
        ws_id: &str,
        entries: impl IntoIterator<Item = (String, SnapshotMeta)>,
    ) {
        let live = self.state.backend.data_root().join(ws_id);
        std::fs::create_dir_all(&live).unwrap();
        let path = self.path(ws_id);
        std::os::unix::fs::symlink(&live, &path).unwrap();
        let mut index = SnapshotIndex::new(path.clone());
        index.snapshots.extend(entries);
        self.state
            .register_workspace(ws_id.to_string(), path, index)
            .unwrap();
    }

    fn path(&self, ws_id: &str) -> PathBuf {
        self.root.path().join(format!("user-{ws_id}"))
    }

    async fn list(&self, scope: Option<&str>, limit: u32, cursor: Option<&str>) -> Response {
        list_page(&self.state, scope, limit, cursor).await.unwrap()
    }
}

fn meta() -> SnapshotMeta {
    SnapshotMeta {
        created_at: DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
        message: None,
        metadata: None,
        pinned: false,
        missing: false,
        parent_id: None,
        child_ids: vec![],
    }
}

fn entries(ids: &[&str]) -> Vec<(String, SnapshotMeta)> {
    ids.iter().map(|id| (id.to_string(), meta())).collect()
}

fn unpack(response: Response) -> (Vec<SnapshotEntry>, Option<String>) {
    match response {
        Response::ListPageOk {
            snapshots,
            next_cursor,
        } => (snapshots, next_cursor),
        response => panic!("expected a page, got {response:?}"),
    }
}

fn assert_error(response: Response, expected: ErrorCode) {
    match response {
        Response::Error { code, .. } => assert_eq!(code, expected),
        response => panic!("expected an error, got {response:?}"),
    }
}

fn unpack_summary(response: Response) -> (SnapshotSummary, Option<String>) {
    assert!(encoded_payload_size(&response).unwrap() <= u64::from(MAX_FRAME_SIZE));
    let frame = encode_frame(&response).unwrap();
    match ws_ckpt_common::decode_payload(&frame[4..]).unwrap() {
        Response::ListPageSummaryOk {
            snapshot,
            next_cursor,
        } => (snapshot, next_cursor),
        response => panic!("expected a summary page, got {response:?}"),
    }
}

fn token(cursor: &Cursor) -> String {
    hex::encode(serde_json::to_vec(cursor).unwrap())
}

#[tokio::test]
async fn static_pages_order_ties_by_workspace_then_snapshot_id() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-b", entries(&["same", "a", "z"]));
    fixture.add_workspace("ws-a", entries(&["z", "same", "a"]));
    let mut cursor = None;
    let mut all = Vec::new();
    for _ in 0..3 {
        let response = fixture.list(None, 2, cursor.as_deref()).await;
        let (snapshots, next) = unpack(response);
        assert_eq!(snapshots.len(), 2);
        assert_ne!(next.as_ref(), cursor.as_ref());
        all.extend(
            snapshots
                .into_iter()
                .map(|entry| (entry.workspace, entry.id)),
        );
        cursor = next;
    }
    assert!(cursor.is_none());
    let expected: Vec<_> = ["ws-a", "ws-b"]
        .into_iter()
        .flat_map(|ws| {
            ["a", "same", "z"].map(|id| {
                (
                    fixture.path(ws).to_string_lossy().into_owned(),
                    id.to_string(),
                )
            })
        })
        .collect();
    assert_eq!(all, expected);
}

#[tokio::test]
async fn cursor_preserves_subsecond_precision_and_alias_scope() {
    let fixture = Fixture::new();
    let mut earlier = meta();
    earlier.created_at -= chrono::Duration::nanoseconds(1);
    fixture.add_workspace("ws-a", [("z".into(), earlier), ("a".into(), meta())]);
    let (first, cursor) = unpack(fixture.list(Some("ws-a"), 1, None).await);
    assert_eq!(first[0].id, "z");
    let decoded = Cursor::decode(cursor.as_ref().unwrap(), Some("ws-a")).unwrap();
    assert_eq!(decoded.after.created_at, first[0].meta.created_at);
    assert_eq!(decoded.upper.created_at, meta().created_at);
    let path = fixture.path("ws-a").to_string_lossy().into_owned();
    let (second, cursor) = unpack(fixture.list(Some(&path), 1, cursor.as_deref()).await);
    assert_eq!(second[0].id, "a");
    assert!(cursor.is_none());
}

#[tokio::test]
async fn invalid_limits_empty_scope_and_detached_registration() {
    let fixture = Fixture::new();
    for limit in [0, MAX_LIST_PAGE_LIMIT + 1, u32::MAX] {
        assert_error(
            fixture.list(None, limit, None).await,
            ErrorCode::InvalidListRequest,
        );
    }
    let (snapshots, cursor) = unpack(fixture.list(None, MAX_LIST_PAGE_LIMIT, None).await);
    assert!(snapshots.is_empty());
    assert!(cursor.is_none());
    assert_error(
        fixture.list(Some("unknown"), 1, None).await,
        ErrorCode::WorkspaceNotFound,
    );
    fixture.add_workspace("ws-a", entries(&["a"]));
    std::fs::remove_file(fixture.path("ws-a")).unwrap();
    let response = fixture.list(Some("ws-a"), 1, None).await;
    assert!(matches!(response, Response::Error { message, .. } if message.contains("recover")));
    // The global list intentionally retains legacy visibility of detached workspaces.
    let (snapshots, cursor) = unpack(fixture.list(None, 1, None).await);
    assert_eq!(snapshots.len(), 1);
    assert!(cursor.is_none());
}

#[tokio::test]
async fn malformed_version_range_and_scope_cursors_are_rejected() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "z"]));
    fixture.add_workspace("ws-b", entries(&["a", "z"]));
    let (_, encoded) = unpack(fixture.list(Some("ws-a"), 1, None).await);
    let encoded = encoded.unwrap();
    let valid = Cursor::decode(&encoded, Some("ws-a")).unwrap();
    let mut invalid = vec![
        String::new(),
        "z0".to_string(),
        "0".to_string(),
        hex::encode("not JSON"),
        hex::encode("{}"),
        "a".repeat(MAX_LIST_CURSOR_BYTES + 1),
    ];
    let mut changed = valid.clone();
    changed.version = 2;
    invalid.push(token(&changed));
    changed = valid.clone();
    changed.after = changed.upper.clone();
    invalid.push(token(&changed));
    changed = valid.clone();
    std::mem::swap(&mut changed.after, &mut changed.upper);
    invalid.push(token(&changed));
    changed = valid.clone();
    changed.after.id.clear();
    invalid.push(token(&changed));
    changed = valid.clone();
    changed.upper.ws_id.clear();
    invalid.push(token(&changed));
    changed = valid.clone();
    changed.scope = Some("ws-b".to_string());
    invalid.push(token(&changed));
    changed = valid.clone();
    changed.after.ws_id = "ws-b".to_string();
    invalid.push(token(&changed));
    changed = valid.clone();
    changed.upper.ws_id = "ws-b".to_string();
    invalid.push(token(&changed));
    let mut extra = serde_json::to_value(&valid).unwrap();
    extra["unexpected"] = true.into();
    invalid.push(hex::encode(serde_json::to_vec(&extra).unwrap()));
    let mut bad_timestamp = serde_json::to_value(&valid).unwrap();
    bad_timestamp["after"]["created_at"] = "invalid".into();
    invalid.push(hex::encode(serde_json::to_vec(&bad_timestamp).unwrap()));
    for encoded in invalid {
        assert_error(
            fixture.list(Some("ws-a"), 1, Some(&encoded)).await,
            ErrorCode::InvalidListCursor,
        );
    }
    for scope in [None, Some("ws-b")] {
        assert_error(
            fixture.list(scope, 1, Some(&encoded)).await,
            ErrorCode::InvalidListCursor,
        );
    }
    let (_, global) = unpack(fixture.list(None, 1, None).await);
    assert_error(
        fixture.list(Some("ws-a"), 1, global.as_deref()).await,
        ErrorCode::InvalidListCursor,
    );
}

#[tokio::test]
async fn deleted_cursor_and_unread_rows_do_not_break_continuation() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "b", "c", "d"]));
    let (_, cursor) = unpack(fixture.list(None, 1, None).await);
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        let mut ws = arc.write().await;
        ws.index.snapshots.remove("a");
        ws.index.snapshots.remove("b");
        ws.index.snapshots.insert("z".to_string(), meta());
        ws.index.snapshots.get_mut("c").unwrap().message = Some("updated".into());
    }
    let (snapshots, cursor) = unpack(fixture.list(None, 10, cursor.as_deref()).await);
    assert_eq!(
        snapshots.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["c", "d"]
    );
    assert_eq!(snapshots[0].meta.message.as_deref(), Some("updated"));
    assert!(cursor.is_none());
}

#[tokio::test]
async fn deletion_of_all_remaining_rows_returns_an_empty_terminal_page() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "z"]));
    let (_, cursor) = unpack(fixture.list(None, 1, None).await);
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        arc.write().await.index.snapshots.clear();
    }
    let (snapshots, cursor) = unpack(fixture.list(None, 1, cursor.as_deref()).await);
    assert!(snapshots.is_empty());
    assert!(cursor.is_none());
}

#[tokio::test]
async fn vanished_selected_batch_refills_without_empty_continuations() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "b", "c", "d", "e"]));
    let selected = select_candidates(&fixture.state, None, None, None, 3).await;
    assert_eq!(selected.keys.len(), 3);
    assert!(selected.has_more);
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        let mut ws = arc.write().await;
        for id in ["a", "b", "c"] {
            ws.index.snapshots.remove(id);
        }
    }
    let (snapshots, cursor) = unpack(
        collect_page(&fixture.state, None, 2, None, selected)
            .await
            .unwrap(),
    );
    assert_eq!(
        snapshots.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["d", "e"]
    );
    assert!(cursor.is_none());
}

#[tokio::test]
async fn changed_key_is_reread_and_refilled_below_the_frozen_upper() {
    let fixture = Fixture::new();
    let mut upper_meta = meta();
    upper_meta.created_at += chrono::Duration::seconds(10);
    fixture.add_workspace("ws-a", [("a".into(), meta()), ("z".into(), upper_meta)]);
    let selected = select_candidates(&fixture.state, None, None, None, 2).await;
    let upper = selected.upper.clone().unwrap();
    // Model a continuation whose previous upper row has since disappeared.
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        arc.write().await.index.snapshots.remove("z");
    }
    let selected = select_candidates(&fixture.state, None, None, Some(&upper), 2).await;
    assert!(!selected.has_more);
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        arc.write()
            .await
            .index
            .snapshots
            .get_mut("a")
            .unwrap()
            .created_at += chrono::Duration::seconds(1);
    }
    let (snapshots, cursor) = unpack(
        collect_page(&fixture.state, None, 1, None, selected)
            .await
            .unwrap(),
    );
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].meta.created_at,
        meta().created_at + chrono::Duration::seconds(1)
    );
    assert!(cursor.is_none());
}

#[tokio::test]
async fn vanished_lookahead_is_not_mistaken_for_more_pages() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "b", "c"]));
    let selected = select_candidates(&fixture.state, None, None, None, 2).await;
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        let mut ws = arc.write().await;
        ws.index.snapshots.remove("b");
        ws.index.snapshots.remove("c");
    }
    let (snapshots, cursor) = unpack(
        collect_page(&fixture.state, None, 1, None, selected)
            .await
            .unwrap(),
    );
    assert_eq!(snapshots.len(), 1);
    assert!(cursor.is_none());
}

#[test]
fn borrowed_entry_and_full_response_size_match_wire_encoding() {
    for metadata in [
        None,
        Some(serde_json::Value::Null),
        Some(serde_json::json!({"nested": [1, "雪", true]})),
    ] {
        let mut snapshot_meta = meta();
        snapshot_meta.metadata = metadata;
        snapshot_meta.message = Some("message with unicode: 雪".into());
        snapshot_meta.pinned = true;
        snapshot_meta.missing = true;
        snapshot_meta.parent_id = Some("parent".into());
        snapshot_meta.child_ids = vec!["first".into(), "second".into()];
        let entry = SnapshotEntry {
            id: "id".into(),
            workspace: "/workspace".into(),
            meta: snapshot_meta,
        };
        let borrowed = (&entry.id, entry.workspace.as_str(), &entry.meta);
        assert_eq!(
            encode_frame(&entry).unwrap(),
            encode_frame(&borrowed).unwrap()
        );
        for cursor in [None, Some("0123456789".to_string())] {
            let measured = encoded_payload_size(&page(Vec::new(), cursor.clone())).unwrap()
                + 2 * encoded_payload_size(&borrowed).unwrap();
            let response = page(vec![entry.clone(), entry.clone()], cursor);
            assert_eq!(measured, encoded_payload_size(&response).unwrap());
            assert_eq!(
                measured as usize + 4,
                encode_frame(&response).unwrap().len()
            );
        }
    }
}

#[tokio::test]
async fn target_budget_counts_the_envelope_and_cursor() {
    let fixture = Fixture::new();
    let mut snapshot_meta = meta();
    snapshot_meta.message = Some("x".repeat(100_000));
    fixture.add_workspace(
        "ws-a",
        (0..20).map(|id| (format!("{id:02}"), snapshot_meta.clone())),
    );
    let response = fixture.list(None, 20, None).await;
    assert!(encoded_payload_size(&response).unwrap() <= LIST_PAGE_TARGET_BYTES);
    let (snapshots, cursor) = unpack(response);
    assert_eq!(snapshots.len(), 10);
    assert!(cursor.is_some());
    let mut one_more = snapshots.clone();
    one_more.push(snapshots[0].clone());
    assert!(
        encoded_payload_size(&page(one_more, cursor.clone())).unwrap() > LIST_PAGE_TARGET_BYTES
    );
    let (rest, cursor) = unpack(fixture.list(None, 20, cursor.as_deref()).await);
    assert_eq!(rest.len(), 10);
    assert!(cursor.is_none());
}

async fn set_first_entry_size(fixture: &Fixture, size: u64) {
    let selected = select_candidates(&fixture.state, None, None, None, 2).await;
    let cursor = token(&Cursor {
        version: 1,
        scope: None,
        after: selected.keys[0].clone(),
        upper: selected.upper.unwrap(),
    });
    let arc = fixture.state.get_by_wsid("ws-a").unwrap();
    let mut ws = arc.write().await;
    let workspace = ws.index.workspace_path.to_string_lossy().into_owned();
    let entry = ws.index.snapshots.get_mut("a").unwrap();
    entry.message = Some(String::new());
    let base = encoded_payload_size(&page(Vec::new(), Some(cursor))).unwrap()
        + encoded_payload_size(&("a", workspace.as_str(), &*entry)).unwrap();
    entry.message = Some("x".repeat(usize::try_from(size - base).unwrap()));
}

#[tokio::test]
async fn exact_target_and_frame_boundaries_include_required_cursor() {
    for budget in [LIST_PAGE_TARGET_BYTES, u64::from(MAX_FRAME_SIZE)] {
        let fixture = Fixture::new();
        fixture.add_workspace("ws-a", entries(&["a", "z"]));
        set_first_entry_size(&fixture, budget).await;
        let response = fixture.list(None, 1, None).await;
        assert_eq!(encoded_payload_size(&response).unwrap(), budget);
        assert_eq!(encode_frame(&response).unwrap().len() as u64, budget + 4);
        let (snapshots, cursor) = unpack(response);
        assert_eq!(snapshots.len(), 1);
        assert!(cursor.is_some());
        let (last, cursor) = unpack(fixture.list(None, 1, cursor.as_deref()).await);
        assert_eq!(last[0].id, "z");
        assert!(cursor.is_none());
    }
}

#[tokio::test]
async fn cursor_can_push_a_single_entry_over_the_frame_limit() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "z"]));
    set_first_entry_size(&fixture, u64::from(MAX_FRAME_SIZE) + 1).await;
    let (summary, cursor) = unpack_summary(fixture.list(None, 1, None).await);
    assert_eq!(summary.id, "a");
    assert_eq!(
        Cursor::decode(cursor.as_ref().unwrap(), None)
            .unwrap()
            .after
            .id,
        "a"
    );
    let (rest, cursor) = unpack(fixture.list(None, 1, cursor.as_deref()).await);
    assert_eq!(rest[0].id, "z");
    assert!(cursor.is_none());
    // The metadata itself fits; removing the need for a cursor makes it legal.
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        arc.write().await.index.snapshots.remove("z");
    }
    let response = fixture.list(None, 1, None).await;
    assert!(encoded_payload_size(&response).unwrap() <= u64::from(MAX_FRAME_SIZE));
    let (snapshots, cursor) = unpack(response);
    assert_eq!(snapshots.len(), 1);
    assert!(cursor.is_none());
}

#[tokio::test]
async fn oversized_singletons_use_summaries_only_above_the_frame_limit() {
    for bytes in [2 * LIST_PAGE_TARGET_BYTES, u64::from(MAX_FRAME_SIZE) + 1] {
        let fixture = Fixture::new();
        let mut snapshot_meta = meta();
        snapshot_meta.message = Some("x".repeat(bytes as usize));
        fixture.add_workspace("ws-a", [("a".into(), snapshot_meta)]);
        let response = fixture.list(None, 10, None).await;
        if bytes > u64::from(MAX_FRAME_SIZE) {
            let (summary, cursor) = unpack_summary(response);
            assert_eq!(summary.id, "a");
            assert!(cursor.is_none());
        } else {
            assert!(encoded_payload_size(&response).unwrap() > LIST_PAGE_TARGET_BYTES);
            assert!(encoded_payload_size(&response).unwrap() <= u64::from(MAX_FRAME_SIZE));
            let (snapshots, cursor) = unpack(response);
            assert_eq!(snapshots.len(), 1);
            assert!(cursor.is_none());
        }
    }
}

#[tokio::test]
async fn later_unframeable_entry_does_not_lose_the_preceding_page() {
    let fixture = Fixture::new();
    let mut too_large = meta();
    too_large.message = Some("x".repeat(MAX_FRAME_SIZE as usize + 1));
    fixture.add_workspace(
        "ws-a",
        [
            ("a".into(), meta()),
            ("b".into(), too_large),
            ("c".into(), meta()),
        ],
    );
    let (snapshots, cursor) = unpack(fixture.list(None, 100, None).await);
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].id, "a");
    assert!(cursor.is_some());
    let (summary, next) = unpack_summary(fixture.list(None, 100, cursor.as_deref()).await);
    assert_eq!(summary.id, "b");
    assert_ne!(next, cursor);
    assert_eq!(
        Cursor::decode(next.as_ref().unwrap(), None)
            .unwrap()
            .after
            .id,
        "b"
    );
    let (rest, cursor) = unpack(fixture.list(None, 100, next.as_deref()).await);
    assert_eq!(
        rest.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["c"]
    );
    assert!(cursor.is_none());
}

#[tokio::test]
async fn summaries_preserve_identity_flags_and_index_for_every_large_field() {
    let fixture = Fixture::new();
    let mut oversized = meta();
    oversized.pinned = true;
    oversized.missing = true;
    let large = "雪".repeat(MAX_FRAME_SIZE as usize / 3 + 1);
    let mut metadata = oversized.clone();
    metadata.metadata = Some(serde_json::json!({"value": large}));
    let mut message = oversized.clone();
    message.message = Some(large.clone());
    let mut parent = oversized.clone();
    parent.parent_id = Some(large.clone());
    let mut children = oversized;
    children.child_ids = vec![large];
    let originals = [
        ("a-metadata".into(), metadata),
        ("b-message".into(), message),
        ("c-parent".into(), parent),
        ("d-children".into(), children),
    ];
    fixture.add_workspace("ws-a", originals.clone());
    for scope in [None, Some("ws-a")] {
        let mut cursor = None;
        for (position, (id, original)) in originals.iter().enumerate() {
            let (summary, next) = unpack_summary(fixture.list(scope, 100, cursor.as_deref()).await);
            assert_eq!(&summary.id, id);
            assert_eq!(summary.workspace, fixture.path("ws-a").to_string_lossy());
            assert_eq!(summary.meta.created_at, original.created_at);
            assert!(summary.meta.pinned);
            assert!(summary.meta.missing);
            assert_eq!(next.is_none(), position == originals.len() - 1);
            if let Some(next) = &next {
                assert_eq!(Cursor::decode(next, scope).unwrap().after.id, *id);
                assert_ne!(Some(next), cursor.as_ref());
            }
            cursor = next;
        }
    }
    let arc = fixture.state.get_by_wsid("ws-a").unwrap();
    let ws = arc.read().await;
    for (id, original) in originals {
        assert_eq!(ws.index.snapshots[&id], original);
    }
}

#[tokio::test]
async fn summary_identity_and_cursor_budgets_still_fail_without_skipping() {
    let fixture = Fixture::new();
    let huge_id = "z".repeat(MAX_FRAME_SIZE as usize);
    fixture.add_workspace("ws-a", [(huge_id, meta())]);
    let response = fixture.list(None, 1, None).await;
    assert!(encode_frame(&response).unwrap().len() < 2048);
    assert_error(response, ErrorCode::ListEntryTooLarge);

    let fixture = Fixture::new();
    let huge_cursor_id = "z".repeat(MAX_LIST_CURSOR_BYTES);
    fixture.add_workspace("ws-a", [("a".into(), meta()), (huge_cursor_id, meta())]);
    for _ in 0..2 {
        assert_error(
            fixture.list(None, 1, None).await,
            ErrorCode::ListEntryTooLarge,
        );
    }
}

#[tokio::test]
async fn summary_budget_includes_identity_envelope_and_cursor() {
    for continuation in [false, true] {
        let fixture = Fixture::new();
        let mut large = meta();
        large.message = Some("x".repeat(MAX_FRAME_SIZE as usize));
        let mut entries = vec![("a".into(), large)];
        if continuation {
            entries.push(("z".into(), meta()));
        }
        fixture.add_workspace("ws-a", entries);
        let baseline = fixture.list(None, 1, None).await;
        assert!(matches!(baseline, Response::ListPageSummaryOk { .. }));
        let base = encoded_payload_size(&baseline).unwrap()
            - fixture.path("ws-a").to_string_lossy().len() as u64;
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        for extra in [0, 1] {
            arc.write().await.index.workspace_path =
                PathBuf::from("x".repeat((u64::from(MAX_FRAME_SIZE) - base + extra) as usize));
            let response = fixture.list(None, 1, None).await;
            if extra == 0 {
                assert_eq!(
                    encoded_payload_size(&response).unwrap(),
                    u64::from(MAX_FRAME_SIZE)
                );
                let (summary, cursor) = unpack_summary(response);
                assert_eq!(summary.id, "a");
                assert_eq!(cursor.is_some(), continuation);
            } else {
                assert_error(response, ErrorCode::ListEntryTooLarge);
            }
        }
    }
}

#[tokio::test]
async fn metadata_flags_and_legacy_list_are_preserved() {
    let fixture = Fixture::new();
    let mut snapshot_meta = meta();
    snapshot_meta.pinned = true;
    snapshot_meta.missing = true;
    snapshot_meta.metadata = Some(serde_json::json!({"agent": "test", "turn": 1}));
    snapshot_meta.parent_id = Some("parent".into());
    snapshot_meta.child_ids = vec!["child".into()];
    fixture.add_workspace("ws-a", [("a".into(), snapshot_meta.clone())]);
    let request = Request::ListPage {
        workspace: Some("ws-a".into()),
        limit: 1,
        cursor: None,
    };
    assert_eq!(
        crate::ops_log::ops_name_from_request(&request),
        Some("list_page")
    );
    let response = crate::dispatcher::dispatch(&fixture.state, request).await;
    let (snapshots, cursor) = unpack(response);
    assert_eq!(snapshots[0].meta, snapshot_meta);
    assert!(cursor.is_none());
    for workspace in [None, Some("ws-a".to_string())] {
        let legacy = crate::dispatcher::dispatch(
            &fixture.state,
            Request::List {
                workspace,
                format: Some("json".into()),
            },
        )
        .await;
        assert!(
            matches!(legacy, Response::ListOk { snapshots: legacy_entries } if legacy_entries == snapshots)
        );
    }
}

#[tokio::test]
async fn inserts_are_filtered_by_key_bounds_not_insertion_time() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", entries(&["a", "z"]));
    let (_, cursor) = unpack(fixture.list(None, 1, None).await);
    {
        let arc = fixture.state.get_by_wsid("ws-a").unwrap();
        let mut ws = arc.write().await;
        for id in ["0", "m", "zz"] {
            ws.index.snapshots.insert(id.into(), meta());
        }
    }
    let (snapshots, next) = unpack(fixture.list(None, 10, cursor.as_deref()).await);
    assert_eq!(
        snapshots.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["m", "z"]
    );
    assert!(next.is_none());
    let (snapshots, _) = unpack(fixture.list(None, 10, None).await);
    assert_eq!(
        snapshots.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["0", "a", "m", "z", "zz"]
    );
}

#[tokio::test]
async fn scan_yields_even_for_filtered_entries_without_releasing_its_iterator_guard() {
    use std::future::Future;
    use std::task::Poll;

    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", (0..10_000).map(|id| (format!("{id:05}"), meta())));
    let after = Key {
        created_at: meta().created_at,
        ws_id: "ws-a".into(),
        id: "99999".into(),
    };
    let arc = fixture.state.get_by_wsid("ws-a").unwrap();
    for boundary in [None, Some(&after)] {
        let mut scan = std::pin::pin!(select_candidates(
            &fixture.state,
            Some("ws-a"),
            boundary,
            None,
            2
        ));
        std::future::poll_fn(|cx| {
            assert!(scan.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(arc.try_write().is_err());
        let selected = scan.await;
        assert_eq!(selected.keys.len(), if boundary.is_some() { 0 } else { 2 });
        assert!(arc.try_write().is_ok());
    }
}

#[tokio::test]
async fn first_page_of_one_hundred_thousand_entries_retains_only_bounded_keys() {
    let fixture = Fixture::new();
    fixture.add_workspace("ws-a", (0..100_000).map(|id| (format!("{id:06}"), meta())));
    let selected = select_candidates(
        &fixture.state,
        None,
        None,
        None,
        DEFAULT_LIST_PAGE_LIMIT as usize + 1,
    )
    .await;
    assert_eq!(selected.keys.len(), 101);
    assert_eq!(selected.keys.first().unwrap().id, "000000");
    assert_eq!(selected.keys.last().unwrap().id, "000100");
    assert_eq!(selected.upper.as_ref().unwrap().id, "099999");
    assert!(selected.has_more);
    let response = collect_page(&fixture.state, None, 100, None, selected)
        .await
        .unwrap();
    assert!(encoded_payload_size(&response).unwrap() <= LIST_PAGE_TARGET_BYTES);
    let (snapshots, cursor) = unpack(response);
    assert_eq!(snapshots.len(), 100);
    assert_eq!(snapshots[0].id, "000000");
    assert_eq!(snapshots[99].id, "000099");
    let cursor = Cursor::decode(cursor.as_ref().unwrap(), None).unwrap();
    assert_eq!(cursor.after.id, "000099");
    assert_eq!(cursor.upper.id, "099999");
}
