use crate::{DataRecord, QueryResponse, RecordsPage, ScoreKind, ScoredRecord};

pub(super) fn records_page_to_query_response(
    mut page: RecordsPage,
    score_kind: ScoreKind,
) -> QueryResponse {
    let hits = page
        .records
        .drain(..)
        .map(|mut record| {
            let raw_score = extract_score(&mut record);
            ScoredRecord {
                record,
                raw_score,
                score_kind: score_kind.clone(),
            }
        })
        .collect();

    QueryResponse::from_hits(hits, page.next_page)
}

fn extract_score(record: &mut DataRecord) -> Option<f64> {
    for key in ["_distance", "_score"] {
        if let Some(value) = record.fields.remove(key) {
            if let Some(score) = value.as_f64() {
                return Some(score);
            }
            if let Some(score) = value.as_i64() {
                return Some(score as f64);
            }
            if let Some(score) = value.as_u64() {
                return Some(score as f64);
            }
        }
    }
    None
}
