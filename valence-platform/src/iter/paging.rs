//! Table paging helpers for the Valence Iter **orchestrator** (dynamic target table name).
//!
//! Row ids are fetched in ascending primary-key order. The **next** page uses a dialect-specific
//! lower-bound filter on the bare id from the previous page's last row.
//!
//! Only ASCII alphanumeric and `_` table names are accepted (defense in depth against injection in
//! dynamic query fragments).

use anyhow::{anyhow, Context};
use valence::__internal::CompiledQuery;
use valence::QueryCore;
use valence::Valence;
use valence::{MEM_ENGINE_ID, SQLITE_ENGINE_ID};

/// Normalize an id cell (string, `table:id`, or `{id: …}` object) to the bare id string expected by
/// [`valence::DatabaseBackend::get_record`].
fn bare_record_id_from_query_cell(value: &serde_json::Value) -> anyhow::Result<String> {
    let s = if let Some(st) = value.as_str() {
        st.to_string()
    } else if let Some(id) = value.get("id").and_then(|v| v.as_str()) {
        id.to_string()
    } else {
        value
            .as_object()
            .filter(|m| m.contains_key("tb") || m.contains_key("id"))
            .map(|_| value.to_string())
            .unwrap_or_else(|| value.to_string().trim_matches('"').to_string())
    };
    let tail = s.rsplit(':').next().unwrap_or(&s).trim();
    Ok(tail.to_string())
}

fn assert_safe_table_name(table: &str) -> anyhow::Result<()> {
    if table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(anyhow!(
            "unsafe table name for iter paging (allowed: [a-zA-Z0-9_]): {:?}",
            table
        ))
    }
}

fn is_surreal_engine(engine_id: &str) -> bool {
    engine_id.contains("surreal")
}

fn is_mem_engine(engine_id: &str) -> bool {
    engine_id == MEM_ENGINE_ID || engine_id.contains("inmemory")
}

fn is_sql_document_engine(engine_id: &str) -> bool {
    engine_id == SQLITE_ENGINE_ID || engine_id.contains("postgres") || engine_id.contains("hybrid")
}

/// Engines without a reliable `WHERE id >` pushdown (mem, and historically redis/mongo/indra).
fn uses_in_process_keyset(engine_id: &str) -> bool {
    !is_surreal_engine(engine_id) && !is_sql_document_engine(engine_id)
}

/// Total row count for a table (best-effort for iter progress).
pub async fn count_table_rows(valence: &Valence, table_name: &str) -> anyhow::Result<i64> {
    assert_safe_table_name(table_name)?;
    let backend = valence
        .backend_for_table(table_name)
        .map_err(|e| anyhow!("{}", e))?;
    let engine = backend.engine_id();
    let q = if is_surreal_engine(engine) {
        CompiledQuery::new(
            format!(
                "SELECT VALUE count FROM (SELECT count() AS count FROM {table_name} GROUP ALL)"
            ),
            vec![],
        )
    } else if is_mem_engine(engine) {
        // Mem's COUNT path keys off `COUNT(` + first `FROM <table>`.
        CompiledQuery::new(format!("SELECT count() AS count FROM {table_name}"), vec![])
    } else if is_sql_document_engine(engine) {
        CompiledQuery::new(
            format!("SELECT COUNT(*) AS count FROM {table_name}"),
            vec![],
        )
    } else {
        CompiledQuery::new(
            format!(
                "SELECT VALUE count FROM (SELECT count() AS count FROM {table_name} GROUP ALL)"
            ),
            vec![],
        )
    };
    let rows = backend
        .execute_compiled_query(&q)
        .await
        .map_err(|e| anyhow!("{}", e))?;
    let v = rows
        .into_iter()
        .next()
        .context("count query returned no rows")?;
    let n = v
        .as_i64()
        .or_else(|| v.as_f64().map(|f| f as i64))
        .or_else(|| v.get("count").and_then(|c| c.as_i64()))
        .context("count query returned non-integer")?;
    Ok(n)
}

async fn fetch_ordered_ids(
    valence: &Valence,
    table_name: &str,
    engine: &str,
    after_bare_id: Option<&str>,
    limit: Option<usize>,
) -> anyhow::Result<(Vec<String>, usize)> {
    let backend = valence
        .backend_for_table(table_name)
        .map_err(|e| anyhow!("{}", e))?;

    let limit_sql = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();

    let (query, params) = if let Some(rid) = after_bare_id.filter(|s| !s.is_empty()) {
        if is_surreal_engine(engine) {
            (
                format!(
                    "SELECT VALUE id FROM {table_name} WHERE id > type::record($tb, $rid) ORDER BY id ASC{limit_sql}"
                ),
                vec![
                    ("tb".to_string(), serde_json::json!(table_name)),
                    ("rid".to_string(), serde_json::json!(rid)),
                ],
            )
        } else if is_sql_document_engine(engine) {
            (
                format!("SELECT id FROM {table_name} WHERE id > $rid ORDER BY id ASC{limit_sql}"),
                vec![("rid".to_string(), serde_json::json!(rid))],
            )
        } else {
            (
                format!("SELECT VALUE id FROM {table_name} ORDER BY id ASC{limit_sql}"),
                vec![],
            )
        }
    } else if is_sql_document_engine(engine) {
        (
            format!("SELECT id FROM {table_name} ORDER BY id ASC{limit_sql}"),
            vec![],
        )
    } else {
        (
            format!("SELECT VALUE id FROM {table_name} ORDER BY id ASC{limit_sql}"),
            vec![],
        )
    };

    let compiled = CompiledQuery::new(query, params);
    let rows = backend
        .execute_compiled_query(&compiled)
        .await
        .map_err(|e| anyhow!("{}", e))?;
    let queried = rows.len();

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let bare_id = bare_record_id_from_query_cell(&r)?;
        if bare_id.is_empty() {
            continue;
        }
        match QueryCore::get_entity(table_name, &bare_id, valence).await {
            Ok(Some(_)) => out.push(bare_id),
            Ok(None) | Err(_) => {}
        }
    }
    Ok((out, queried))
}

/// Return up to `limit` **bare** primary keys (tail after `:`) in ascending `id` order.
///
/// `after_bare_id`: bare id of the last row from the **previous** page, or `None` for the first page.
///
/// For engines without `WHERE id >` pushdown (mem and similar), loads the ordered id list (no
/// LIMIT) and applies the keyset cursor in process so pages past the first are not empty.
pub async fn page_row_ids(
    valence: &Valence,
    table_name: &str,
    after_bare_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<Vec<String>> {
    assert_safe_table_name(table_name)?;
    if limit == 0 {
        return Ok(Vec::new());
    }
    let backend = valence
        .backend_for_table(table_name)
        .map_err(|e| anyhow!("{}", e))?;
    let engine = backend.engine_id().to_string();

    if uses_in_process_keyset(&engine) {
        let after = after_bare_id.filter(|s| !s.is_empty());
        // Full ordered scan: mem ORDER BY + LIMIT on a partial HashMap window is not a reliable
        // keyset (LIMIT truncates before a stable total order is guaranteed across calls).
        let (mut raw, _) = fetch_ordered_ids(valence, table_name, &engine, None, None).await?;
        raw.sort();
        raw.dedup();
        if let Some(after_id) = after {
            raw.retain(|id| id.as_str() > after_id);
        }
        raw.truncate(limit);
        return Ok(raw);
    }

    let (ids, _) =
        fetch_ordered_ids(valence, table_name, &engine, after_bare_id, Some(limit)).await?;
    Ok(ids)
}
