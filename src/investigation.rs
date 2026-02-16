use crate::bedrock::{BedrockClientTrait, BedrockUsage};
use crate::bedrock_spend::{
    fetch_bedrock_pricing_from_aws, is_missing_pricing_error,
    record_bedrock_spend, upsert_bedrock_pricing, SpendLogRequest,
};
use crate::hybrid_search;
use crate::AppState;
use anyhow::{anyhow, Context, Result};
use chrono::{Duration, Utc};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub const INVESTIGATION_SPEND_CATEGORY: &str = "video_investigation_search";
const RETRIEVAL_POOL_LIMIT: usize = 30;
const UI_RESULT_LIMIT: usize = 10;

#[derive(Debug, Deserialize)]
pub struct InvestigationRequest {
    pub question: String,
    #[serde(default)]
    pub chat_history: Vec<ChatMessage>,
    #[serde(default)]
    pub search_mode: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct InvestigationResponse {
    pub answer: String,
    pub evidence: Vec<EvidenceMoment>,
    pub search_window: SearchWindow,
    pub usage: InvestigationUsage,
}

#[derive(Debug, Serialize)]
pub struct SearchWindow {
    pub start_utc: String,
    pub end_utc: String,
}

#[derive(Debug, Serialize)]
pub struct InvestigationUsage {
    pub category: String,
    pub model_id: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct EvidenceMoment {
    pub event_id: String,
    pub camera_id: String,
    pub video_path: Option<String>,
    pub snippet: String,
    pub event_start_utc: String,
    pub event_end_utc: String,
    pub event_start_local: String,
    pub event_end_local: String,
    pub start_offset_sec: f64,
    pub end_offset_sec: f64,
    pub jump_url: String,
}

#[derive(Debug)]
struct CandidateMoment {
    evidence: EvidenceMoment,
    rank_score: f64,
}

#[derive(Debug, Deserialize)]
struct PlannerResponse {
    search_queries: Vec<String>,
    time_window_start_utc: Option<String>,
    time_window_end_utc: Option<String>,
    #[serde(default)]
    tool_calls: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AnswerResponse {
    answer: String,
    #[serde(default)]
    evidence_event_ids: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SearchPlanEvent {
    pub search_queries: Vec<String>,
    pub time_window_start_utc: String,
    pub time_window_end_utc: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ToolUseEvent {
    pub tool_name: String,
    pub stage: String,
    pub message: String,
}

pub async fn investigate_question(
    state: &Arc<AppState>,
    request: InvestigationRequest,
) -> Result<InvestigationResponse> {
    let search_mode =
        hybrid_search::SearchMode::parse(request.search_mode.as_deref())
            .ok_or_else(|| {
                anyhow!("search_mode must be one of: hybrid, bm25, vector")
            })?;
    let client = state
        .bedrock_client
        .as_ref()
        .ok_or_else(|| anyhow!("Bedrock client not configured"))?
        .clone();
    let model_id = state.investigation_model.clone();

    let now = Utc::now();
    let timezone = state.timezone.to_string().replace("::", "/");
    let planner_system_prompt =
        "You are an investigation planner for home video transcripts.
Return only JSON with:
{
  \"search_queries\": [\"...\"],
  \"time_window_start_utc\": \"ISO8601\",
  \"time_window_end_utc\": \"ISO8601\",
  \"tool_calls\": [\"get_current_time\"]
}
Rules:
- Use get_current_time tool before resolving words like yesterday/last month.
- No markdown, no prose.
- Keep search_queries concise, max 4.";

    let history_prompt = format_chat_history(&request.chat_history);
    let planner_user_prompt = format!(
        "Tool output get_current_time: now_utc={}, timezone={}\nConversation history:\n{}\nQuestion: {}",
        now.to_rfc3339(),
        timezone,
        history_prompt,
        request.question
    );

    let planner_completion = invoke_and_log_bedrock(
        state,
        client.clone(),
        &model_id,
        "investigation_planner",
        planner_system_prompt,
        &planner_user_prompt,
        600,
    )
    .await?;

    let planner: PlannerResponse = parse_json_body(&planner_completion.content)
        .context("invalid planner json response")?;

    let default_start = now - Duration::days(7);
    let window_start = planner
        .time_window_start_utc
        .as_deref()
        .and_then(parse_utc_timestamp)
        .unwrap_or(default_start);
    let window_end = planner
        .time_window_end_utc
        .as_deref()
        .and_then(parse_utc_timestamp)
        .unwrap_or(now);
    let (window_start, window_end) = if window_start <= window_end {
        (window_start, window_end)
    } else {
        (window_end, window_start)
    };

    let mut search_queries: Vec<String> = planner
        .search_queries
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .take(4)
        .collect();
    if search_queries.is_empty() {
        search_queries.push(request.question.clone());
    }

    let search_params = CandidateSearchParams {
        window_start: window_start.timestamp() as f64,
        window_end: window_end.timestamp() as f64,
        limit: RETRIEVAL_POOL_LIMIT,
        timezone: state.timezone,
        search_mode,
    };
    let mut candidates =
        collect_candidates(state, &search_queries, &search_params)?;

    if candidates.is_empty() {
        let usage = aggregate_usage(
            &model_id,
            &[planner_completion.usage],
            planner_completion.estimated_cost_usd,
        );
        return Ok(InvestigationResponse {
            answer: "I could not find matching moments in the available transcripts for that question."
                .to_string(),
            evidence: Vec::new(),
            search_window: SearchWindow {
                start_utc: window_start.to_rfc3339(),
                end_utc: window_end.to_rfc3339(),
            },
            usage,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .rank_score
            .partial_cmp(&left.rank_score)
            .unwrap_or(Ordering::Equal)
    });
    candidates.truncate(RETRIEVAL_POOL_LIMIT);

    let candidate_json: Vec<Value> = candidates
        .iter()
        .map(|candidate| {
            serde_json::json!({
                "event_id": candidate.evidence.event_id,
                "camera_id": candidate.evidence.camera_id,
                "snippet": candidate.evidence.snippet,
                "event_start_local": candidate.evidence.event_start_local,
                "event_end_local": candidate.evidence.event_end_local,
                "timezone": timezone,
                "start_offset_sec": candidate.evidence.start_offset_sec,
                "end_offset_sec": candidate.evidence.end_offset_sec
            })
        })
        .collect();

    let synthesis_system_prompt =
        "You are a safety video investigation assistant.
Return only JSON with:
{
  \"answer\": \"...\",
  \"evidence_event_ids\": [\"event-id\"]
}
Rules:
- Only use provided candidate evidence.
- Provide the best available answer from candidate evidence, even if imperfect.
- When mentioning dates or times, use server-local time in the provided timezone, not UTC.
- Resolve relative time phrases (for example: yesterday, last week, last month, last year) relative to the provided now_utc value.
- Do not guess the current date from candidate event timestamps.
- If the user includes an explicit year in parentheses (for example: last year (2025)), treat that explicit year as authoritative.
- evidence_event_ids max 10.";
    let synthesis_user_prompt = format!(
        "Temporal context: now_utc={}, timezone={}, resolved_search_window_start_utc={}, resolved_search_window_end_utc={}\nConversation history:\n{}\nQuestion: {}\nPlanner tool calls: {:?}\nCandidates JSON: {}",
        now.to_rfc3339(),
        timezone,
        window_start.to_rfc3339(),
        window_end.to_rfc3339(),
        history_prompt,
        request.question,
        planner.tool_calls,
        serde_json::to_string(&candidate_json)?
    );

    let synthesis_completion = invoke_and_log_bedrock(
        state,
        client,
        &model_id,
        "investigation_answer",
        synthesis_system_prompt,
        &synthesis_user_prompt,
        800,
    )
    .await?;
    let answer_response: AnswerResponse =
        parse_json_body(&synthesis_completion.content)
            .context("invalid synthesis json response")?;

    let candidates_by_event_id: HashMap<String, CandidateMoment> = candidates
        .into_iter()
        .map(|candidate| (candidate.evidence.event_id.clone(), candidate))
        .collect();

    let mut evidence = Vec::new();
    let mut seen = HashSet::new();
    for event_id in answer_response.evidence_event_ids {
        if seen.contains(&event_id) {
            continue;
        }
        if let Some(candidate) = candidates_by_event_id.get(&event_id) {
            evidence.push(candidate.evidence.clone());
            seen.insert(event_id);
        }
        if evidence.len() >= UI_RESULT_LIMIT {
            break;
        }
    }

    if evidence.is_empty() {
        let mut fallback: Vec<&CandidateMoment> =
            candidates_by_event_id.values().collect();
        fallback.sort_by(|left, right| {
            right
                .rank_score
                .partial_cmp(&left.rank_score)
                .unwrap_or(Ordering::Equal)
        });
        for candidate in fallback.into_iter().take(UI_RESULT_LIMIT) {
            evidence.push(candidate.evidence.clone());
        }
    }

    let usage = aggregate_usage(
        &model_id,
        &[planner_completion.usage, synthesis_completion.usage],
        planner_completion.estimated_cost_usd
            + synthesis_completion.estimated_cost_usd,
    );

    Ok(InvestigationResponse {
        answer: answer_response.answer,
        evidence,
        search_window: SearchWindow {
            start_utc: window_start.to_rfc3339(),
            end_utc: window_end.to_rfc3339(),
        },
        usage,
    })
}

pub async fn investigate_question_streaming(
    state: &Arc<AppState>,
    request: InvestigationRequest,
    on_search_plan: &mut (dyn FnMut(SearchPlanEvent) + Send),
    on_tool_use: &mut (dyn FnMut(ToolUseEvent) + Send),
    on_delta: &mut (dyn FnMut(String) + Send),
) -> Result<InvestigationResponse> {
    let search_mode =
        hybrid_search::SearchMode::parse(request.search_mode.as_deref())
            .ok_or_else(|| {
                anyhow!("search_mode must be one of: hybrid, bm25, vector")
            })?;
    let client = state
        .bedrock_client
        .as_ref()
        .ok_or_else(|| anyhow!("Bedrock client not configured"))?
        .clone();
    let model_id = state.investigation_model.clone();

    let now = Utc::now();
    let timezone = state.timezone.to_string().replace("::", "/");
    let planner_system_prompt =
        "You are an investigation planner for home video transcripts.
Return only JSON with:
{
  \"search_queries\": [\"...\"],
  \"time_window_start_utc\": \"ISO8601\",
  \"time_window_end_utc\": \"ISO8601\",
  \"tool_calls\": [\"get_current_time\"]
}
Rules:
- Use get_current_time tool before resolving words like yesterday/last month.
- No markdown, no prose.
- Keep search_queries concise, max 4.";

    let history_prompt = format_chat_history(&request.chat_history);
    on_tool_use(ToolUseEvent {
        tool_name: "get_current_time".to_string(),
        stage: "completed".to_string(),
        message: format!("now_utc={} timezone={}", now.to_rfc3339(), timezone),
    });
    let planner_user_prompt = format!(
        "Tool output get_current_time: now_utc={}, timezone={}\nConversation history:\n{}\nQuestion: {}",
        now.to_rfc3339(),
        timezone,
        history_prompt,
        request.question
    );

    on_tool_use(ToolUseEvent {
        tool_name: "bedrock_planner".to_string(),
        stage: "started".to_string(),
        message: "Planning search strategy".to_string(),
    });
    let planner_completion = invoke_and_log_bedrock(
        state,
        client.clone(),
        &model_id,
        "investigation_planner",
        planner_system_prompt,
        &planner_user_prompt,
        600,
    )
    .await?;

    let planner: PlannerResponse = parse_json_body(&planner_completion.content)
        .context("invalid planner json response")?;
    on_tool_use(ToolUseEvent {
        tool_name: "bedrock_planner".to_string(),
        stage: "completed".to_string(),
        message: format!(
            "Generated {} query terms and {} planner tool calls",
            planner.search_queries.len(),
            planner.tool_calls.len()
        ),
    });

    let default_start = now - Duration::days(7);
    let window_start = planner
        .time_window_start_utc
        .as_deref()
        .and_then(parse_utc_timestamp)
        .unwrap_or(default_start);
    let window_end = planner
        .time_window_end_utc
        .as_deref()
        .and_then(parse_utc_timestamp)
        .unwrap_or(now);
    let (window_start, window_end) = if window_start <= window_end {
        (window_start, window_end)
    } else {
        (window_end, window_start)
    };

    let mut search_queries: Vec<String> = planner
        .search_queries
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .take(4)
        .collect();
    if search_queries.is_empty() {
        search_queries.push(request.question.clone());
    }
    on_search_plan(SearchPlanEvent {
        search_queries: search_queries.clone(),
        time_window_start_utc: window_start.to_rfc3339(),
        time_window_end_utc: window_end.to_rfc3339(),
    });
    on_tool_use(ToolUseEvent {
        tool_name: "transcript_search".to_string(),
        stage: "started".to_string(),
        message: format!(
            "Searching {} query terms in window {} to {}",
            search_queries.len(),
            window_start.to_rfc3339(),
            window_end.to_rfc3339()
        ),
    });
    let search_params = CandidateSearchParams {
        window_start: window_start.timestamp() as f64,
        window_end: window_end.timestamp() as f64,
        limit: RETRIEVAL_POOL_LIMIT,
        timezone: state.timezone,
        search_mode,
    };
    let mut candidates = collect_candidates_with_progress(
        state,
        &search_queries,
        &search_params,
        |query_term, hit_count| {
            on_tool_use(ToolUseEvent {
                tool_name: "transcript_search".to_string(),
                stage: "query".to_string(),
                message: format!("query='{}' hits={}", query_term, hit_count),
            });
        },
    )?;
    on_tool_use(ToolUseEvent {
        tool_name: "transcript_search".to_string(),
        stage: "completed".to_string(),
        message: format!(
            "Collected {} unique candidate moments",
            candidates.len()
        ),
    });

    if candidates.is_empty() {
        let usage = aggregate_usage(
            &model_id,
            &[planner_completion.usage],
            planner_completion.estimated_cost_usd,
        );
        return Ok(InvestigationResponse {
            answer: "I could not find matching moments in the available transcripts for that question."
                .to_string(),
            evidence: Vec::new(),
            search_window: SearchWindow {
                start_utc: window_start.to_rfc3339(),
                end_utc: window_end.to_rfc3339(),
            },
            usage,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .rank_score
            .partial_cmp(&left.rank_score)
            .unwrap_or(Ordering::Equal)
    });
    candidates.truncate(RETRIEVAL_POOL_LIMIT);

    let candidate_json: Vec<Value> = candidates
        .iter()
        .map(|candidate| {
            serde_json::json!({
                "event_id": candidate.evidence.event_id,
                "camera_id": candidate.evidence.camera_id,
                "snippet": candidate.evidence.snippet,
                "event_start_local": candidate.evidence.event_start_local,
                "event_end_local": candidate.evidence.event_end_local,
                "timezone": timezone,
                "start_offset_sec": candidate.evidence.start_offset_sec,
                "end_offset_sec": candidate.evidence.end_offset_sec
            })
        })
        .collect();

    let synthesis_system_prompt =
        "You are a safety video investigation assistant.
Return only JSON with:
{
  \"answer\": \"...\",
  \"evidence_event_ids\": [\"event-id\"]
}
Rules:
- Only use provided candidate evidence.
- Provide the best available answer from candidate evidence, even if imperfect.
- When mentioning dates or times, use server-local time in the provided timezone, not UTC.
- Resolve relative time phrases (for example: yesterday, last week, last month, last year) relative to the provided now_utc value.
- Do not guess the current date from candidate event timestamps.
- If the user includes an explicit year in parentheses (for example: last year (2025)), treat that explicit year as authoritative.
- evidence_event_ids max 10.";
    let synthesis_user_prompt = format!(
        "Temporal context: now_utc={}, timezone={}, resolved_search_window_start_utc={}, resolved_search_window_end_utc={}\nConversation history:\n{}\nQuestion: {}\nPlanner tool calls: {:?}\nCandidates JSON: {}",
        now.to_rfc3339(),
        timezone,
        window_start.to_rfc3339(),
        window_end.to_rfc3339(),
        history_prompt,
        request.question,
        planner.tool_calls,
        serde_json::to_string(&candidate_json)?
    );

    on_tool_use(ToolUseEvent {
        tool_name: "bedrock_synthesis".to_string(),
        stage: "started".to_string(),
        message: "Synthesizing final answer from candidates".to_string(),
    });
    let synthesis_completion = invoke_and_log_bedrock_streaming(
        state,
        client,
        &model_id,
        BedrockCompletionRequest {
            operation: "investigation_answer",
            system_prompt: synthesis_system_prompt,
            user_prompt: &synthesis_user_prompt,
            max_tokens: 800,
        },
        on_delta,
    )
    .await?;
    on_tool_use(ToolUseEvent {
        tool_name: "bedrock_synthesis".to_string(),
        stage: "completed".to_string(),
        message: "Answer generation completed".to_string(),
    });
    let answer_response: AnswerResponse =
        parse_json_body(&synthesis_completion.content)
            .context("invalid synthesis json response")?;

    let candidates_by_event_id: HashMap<String, CandidateMoment> = candidates
        .into_iter()
        .map(|candidate| (candidate.evidence.event_id.clone(), candidate))
        .collect();

    let mut evidence = Vec::new();
    let mut seen = HashSet::new();
    for event_id in answer_response.evidence_event_ids {
        if seen.contains(&event_id) {
            continue;
        }
        if let Some(candidate) = candidates_by_event_id.get(&event_id) {
            evidence.push(candidate.evidence.clone());
            seen.insert(event_id);
        }
        if evidence.len() >= UI_RESULT_LIMIT {
            break;
        }
    }

    if evidence.is_empty() {
        let mut fallback: Vec<&CandidateMoment> =
            candidates_by_event_id.values().collect();
        fallback.sort_by(|left, right| {
            right
                .rank_score
                .partial_cmp(&left.rank_score)
                .unwrap_or(Ordering::Equal)
        });
        for candidate in fallback.into_iter().take(UI_RESULT_LIMIT) {
            evidence.push(candidate.evidence.clone());
        }
    }

    let usage = aggregate_usage(
        &model_id,
        &[planner_completion.usage, synthesis_completion.usage],
        planner_completion.estimated_cost_usd
            + synthesis_completion.estimated_cost_usd,
    );

    Ok(InvestigationResponse {
        answer: answer_response.answer,
        evidence,
        search_window: SearchWindow {
            start_utc: window_start.to_rfc3339(),
            end_utc: window_end.to_rfc3339(),
        },
        usage,
    })
}

fn aggregate_usage(
    model_id: &str,
    usage_values: &[BedrockUsage],
    estimated_cost_usd: f64,
) -> InvestigationUsage {
    let input_tokens: i64 =
        usage_values.iter().map(|usage| usage.input_tokens).sum();
    let output_tokens: i64 =
        usage_values.iter().map(|usage| usage.output_tokens).sum();
    InvestigationUsage {
        category: INVESTIGATION_SPEND_CATEGORY.to_string(),
        model_id: model_id.to_string(),
        input_tokens,
        output_tokens,
        estimated_cost_usd,
    }
}

fn parse_utc_timestamp(value: &str) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn format_chat_history(history: &[ChatMessage]) -> String {
    if history.is_empty() {
        return "(none)".to_string();
    }
    history
        .iter()
        .rev()
        .take(10)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|msg| {
            let role = msg.role.trim().to_lowercase();
            let content = msg.content.trim().replace('\n', " ");
            format!("{role}: {content}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_json_body<T: for<'de> Deserialize<'de>>(value: &str) -> Result<T> {
    if let Ok(parsed) = serde_json::from_str::<T>(value) {
        return Ok(parsed);
    }

    let start_index = value.find('{').ok_or_else(|| anyhow!("missing {{"))?;
    let end_index = value.rfind('}').ok_or_else(|| anyhow!("missing }}"))?;
    if start_index >= end_index {
        return Err(anyhow!("malformed json object boundaries"));
    }
    let trimmed = &value[start_index..=end_index];
    Ok(serde_json::from_str(trimmed)?)
}

struct CandidateSearchParams {
    window_start: f64,
    window_end: f64,
    limit: usize,
    timezone: chrono_tz::Tz,
    search_mode: hybrid_search::SearchMode,
}

fn collect_candidates(
    state: &Arc<AppState>,
    search_queries: &[String],
    params: &CandidateSearchParams,
) -> Result<Vec<CandidateMoment>> {
    collect_candidates_with_progress(
        state,
        search_queries,
        params,
        |_query_term, _hit_count| {},
    )
}

fn collect_candidates_with_progress<F>(
    state: &Arc<AppState>,
    search_queries: &[String],
    params: &CandidateSearchParams,
    mut on_query_results: F,
) -> Result<Vec<CandidateMoment>>
where
    F: FnMut(&str, usize),
{
    let conn = state.zumblezay_db.get()?;
    let mut candidates = Vec::new();
    let per_query_limit =
        usize::max(10, params.limit / usize::max(1, search_queries.len()));

    for query_term in search_queries {
        let filters = hybrid_search::EventFilters {
            transcription_type: state.transcription_service.clone(),
            start_ts: params.window_start,
            end_ts: params.window_end,
            camera_id: None,
            cursor_start: None,
            cursor_event_id: None,
        };
        let hits = hybrid_search::search_events_with_client(
            &conn,
            query_term,
            &filters,
            per_query_limit,
            params.search_mode,
            state.bedrock_client.clone(),
        )?;

        let mut query_hits = 0;
        for hit in hits {
            let row = conn
                .query_row(
                    "SELECT
                        e.camera_id,
                        e.video_path,
                        e.event_start,
                        e.event_end,
                        t.raw_response,
                        transcript_search.content
                     FROM events e
                     JOIN transcriptions t ON t.event_id = e.event_id
                     LEFT JOIN transcript_search
                       ON transcript_search.event_id = e.event_id
                     WHERE e.event_id = ?
                       AND t.transcription_type = ?",
                    params![
                        hit.event_id,
                        state.transcription_service.as_str(),
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, f64>(2)?,
                            row.get::<_, f64>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, Option<String>>(5)?
                                .unwrap_or_default(),
                        ))
                    },
                )
                .optional()?;
            let Some((
                camera_id,
                video_path,
                event_start,
                event_end,
                raw_response,
                indexed_content,
            )) = row
            else {
                continue;
            };

            let (snippet, start_offset_sec, end_offset_sec) =
                match (hit.segment_start_ms, hit.segment_end_ms) {
                    (Some(start_ms), Some(end_ms)) => (
                        hit.snippet.clone(),
                        start_ms as f64 / 1000.0,
                        end_ms as f64 / 1000.0,
                    ),
                    _ => extract_best_snippet(&raw_response, query_term)
                        .unwrap_or_else(|| fallback_snippet(&indexed_content)),
                };
            candidates.push(CandidateMoment {
                evidence: EvidenceMoment {
                    event_id: hit.event_id.clone(),
                    camera_id,
                    video_path,
                    snippet,
                    event_start_utc: timestamp_to_rfc3339(event_start),
                    event_end_utc: timestamp_to_rfc3339(event_end),
                    event_start_local: timestamp_to_local_rfc3339(
                        event_start,
                        params.timezone,
                    ),
                    event_end_local: timestamp_to_local_rfc3339(
                        event_end,
                        params.timezone,
                    ),
                    start_offset_sec,
                    end_offset_sec,
                    jump_url: format!(
                        "/video/{}#t={:.0}",
                        hit.event_id, start_offset_sec
                    ),
                },
                rank_score: hit.score,
            });
            query_hits += 1;
        }
        on_query_results(query_term, query_hits);
    }

    let mut deduped: HashMap<String, CandidateMoment> = HashMap::new();
    for candidate in candidates {
        match deduped.get(&candidate.evidence.event_id) {
            Some(existing) if existing.rank_score >= candidate.rank_score => {}
            _ => {
                deduped.insert(candidate.evidence.event_id.clone(), candidate);
            }
        }
    }
    Ok(deduped.into_values().collect())
}

fn timestamp_to_rfc3339(value: f64) -> String {
    chrono::DateTime::<Utc>::from_timestamp(value as i64, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339())
}

fn timestamp_to_local_rfc3339(value: f64, timezone: chrono_tz::Tz) -> String {
    chrono::DateTime::<Utc>::from_timestamp(value as i64, 0)
        .map(|value| value.with_timezone(&timezone).to_rfc3339())
        .unwrap_or_else(|| Utc::now().with_timezone(&timezone).to_rfc3339())
}

fn fallback_snippet(content: &str) -> (String, f64, f64) {
    let snippet = content.trim().chars().take(220).collect::<String>();
    (snippet, 0.0, 15.0)
}

fn extract_best_snippet(
    raw_response: &str,
    query: &str,
) -> Option<(String, f64, f64)> {
    let parsed: Value = serde_json::from_str(raw_response).ok()?;
    let query_tokens: Vec<String> = query
        .to_lowercase()
        .split_whitespace()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let mut best: Option<(String, f64, f64, usize)> = None;

    if let Some(segments) = parsed.get("segments").and_then(Value::as_array) {
        for segment in segments {
            let text = segment
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if text.is_empty() {
                continue;
            }
            let lower = text.to_lowercase();
            let match_count = query_tokens
                .iter()
                .filter(|token| lower.contains(token.as_str()))
                .count();
            let start =
                segment.get("start").and_then(Value::as_f64).unwrap_or(0.0);
            let end = segment
                .get("end")
                .and_then(Value::as_f64)
                .unwrap_or(start + 15.0);

            match &best {
                Some((_, _, _, best_count)) if *best_count >= match_count => {}
                _ => {
                    best = Some((text, start, end, match_count));
                }
            }
        }
    }

    if let Some((text, start, end, _)) = best {
        return Some((text, start, end));
    }

    parsed
        .get("text")
        .and_then(Value::as_str)
        .map(|text| (text.trim().to_string(), 0.0, 15.0))
        .filter(|(text, _, _)| !text.is_empty())
}

struct LoggedCompletion {
    usage: BedrockUsage,
    content: String,
    estimated_cost_usd: f64,
}

struct BedrockCompletionRequest<'a> {
    operation: &'a str,
    system_prompt: &'a str,
    user_prompt: &'a str,
    max_tokens: i32,
}

async fn invoke_and_log_bedrock(
    state: &Arc<AppState>,
    client: Arc<dyn BedrockClientTrait>,
    model_id: &str,
    operation: &str,
    system_prompt: &str,
    user_prompt: &str,
    max_tokens: i32,
) -> Result<LoggedCompletion> {
    let completion = client
        .complete_text(model_id, system_prompt, user_prompt, max_tokens)
        .await?;

    let conn = state.zumblezay_db.get()?;
    let spend_request = SpendLogRequest {
        category: INVESTIGATION_SPEND_CATEGORY,
        operation,
        model_id,
        request_id: completion.request_id.as_deref(),
        usage: completion.usage.clone(),
    };
    let estimated_cost_usd =
        match record_bedrock_spend(&conn, spend_request.clone()) {
            Ok(value) => value,
            Err(error) if is_missing_pricing_error(&error) => {
                if let Some(rate) = fetch_bedrock_pricing_from_aws(
                    model_id,
                    state.bedrock_region.as_deref(),
                )
                .await?
                {
                    upsert_bedrock_pricing(&conn, model_id, rate)?;
                    record_bedrock_spend(&conn, spend_request)?
                } else {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        };

    Ok(LoggedCompletion {
        usage: completion.usage,
        content: completion.content,
        estimated_cost_usd,
    })
}

async fn invoke_and_log_bedrock_streaming(
    state: &Arc<AppState>,
    client: Arc<dyn BedrockClientTrait>,
    model_id: &str,
    request: BedrockCompletionRequest<'_>,
    on_delta: &mut (dyn FnMut(String) + Send),
) -> Result<LoggedCompletion> {
    let completion = client
        .complete_text_streaming(
            model_id,
            request.system_prompt,
            request.user_prompt,
            request.max_tokens,
            on_delta,
        )
        .await?;

    let conn = state.zumblezay_db.get()?;
    let spend_request = SpendLogRequest {
        category: INVESTIGATION_SPEND_CATEGORY,
        operation: request.operation,
        model_id,
        request_id: completion.request_id.as_deref(),
        usage: completion.usage.clone(),
    };
    let estimated_cost_usd =
        match record_bedrock_spend(&conn, spend_request.clone()) {
            Ok(value) => value,
            Err(error) if is_missing_pricing_error(&error) => {
                if let Some(rate) = fetch_bedrock_pricing_from_aws(
                    model_id,
                    state.bedrock_region.as_deref(),
                )
                .await?
                {
                    upsert_bedrock_pricing(&conn, model_id, rate)?;
                    record_bedrock_spend(&conn, spend_request)?
                } else {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        };

    Ok(LoggedCompletion {
        usage: completion.usage,
        content: completion.content,
        estimated_cost_usd,
    })
}
