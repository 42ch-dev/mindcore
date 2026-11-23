//! The S1–S8 first-mind story: memory, adoption, doubt, evidence, reopen and scope refusals.
//!
//! The story scenario and every S1–S8 assertion live in `support/embedded_story.rs`, shared with
//! `examples/iteration_01.rs`; this file adds only the extra refusal coverage below it.

#[path = "support/embedded_story.rs"]
mod embedded_story;

use std::collections::HashMap;

use mindcore_base::knowledge::{adoption_anchor, KnowledgeScope};
use mindcore_sqlite::MindStore;
use serde_json::{json, Map};
use spoke_operations::{KnowledgeEntryPort, SpokeRejectCode};
use spoke_schemas::knowledge_entry::{ExtensionKey, KnowledgeEntryOwner};

use embedded_story::{entry, expect_ok, expect_reject, json_of, run_story, TempDb, SOURCE_SCOPE};

#[tokio::test]
async fn first_mind_story() {
    run_story("story").await;
}

#[tokio::test]
async fn refusals_leave_state_unchanged() {
    let temp = TempDb::new("refusals");
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));

    let mut adoption = entry(
        "ke_wren_north_gate",
        "location",
        "Wren believes the north gate is shut after dusk.",
    );
    adoption.adoption_anchor = adoption_anchor(SOURCE_SCOPE, "ke_shared_north_gate", 1);
    let personal = expect_ok(
        wren.put_knowledge_entry(adoption, None).await,
        "write the personal entry",
    );

    let mut restated = personal.clone();
    restated.body.summary =
        Some("Wren's note: the north gate is shut after dusk (studio viewpoint).".to_string());
    restated.owner =
        Some(KnowledgeEntryOwner::try_from("ke_studio_record_office").expect("non-empty owner"));
    restated.extensions = HashMap::from([(
        ExtensionKey::try_from("mindcore_test").expect("valid extension key"),
        Map::from_iter([("viewpoint".to_string(), json!("studio"))]),
    )]);
    let updated = expect_ok(
        wren.put_knowledge_entry(restated, Some(1)).await,
        "restate Wren's entry",
    );
    assert_eq!(updated.revision, Some(2));

    assert_eq!(
        expect_reject(
            studio.get_knowledge_entry("ke_wren_north_gate").await,
            "Studio read of Wren's entry",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    assert_eq!(
        expect_reject(
            studio.put_knowledge_entry(updated.clone(), Some(2)).await,
            "Studio write at Wren's revision",
        ),
        SpokeRejectCode::StoredRevisionStale
    );
    let mut different = personal.clone();
    different.body.summary = Some("A different claim about the north gate.".to_string());
    assert_eq!(
        expect_reject(
            wren.put_knowledge_entry(different, Some(1)).await,
            "Wren write at the pre-update revision",
        ),
        SpokeRejectCode::StoredRevisionStale
    );

    let wren_final = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "Wren read after the refusals",
    );
    assert_eq!(json_of(&wren_final), json_of(&updated));
    assert_eq!(wren_final.revision, Some(2));
    assert_eq!(
        expect_reject(
            studio.get_knowledge_entry("ke_wren_north_gate").await,
            "Studio read after the refusals",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    assert!(expect_ok(
        studio.list_entries().await,
        "list studio after the refusals"
    )
    .is_empty());

    store.close().await;
}
