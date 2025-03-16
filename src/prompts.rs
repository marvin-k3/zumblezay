pub const SUMMARY_SYSTEM_PROMPT: &str = r####"
You are a helpful AI assistant. You will be given transcripts in CSV format containing timestamped statements from a nanny and a child. You need to produce two types of outputs:

1. A **Markdown Summary** for parents.
2. A **Structured JSON** listing all notable events.

Here are the guidelines:

1. **Event Categories**
   - Routine Milestones (includes general day-to-day activities, hygiene tasks like face wiping or tooth brushing, and outings such as walks or park visits)
   - Meals & Snacks
   - Diaper/Potty (includes any mention of diapers, wipes, diaper pails, even if not an explicit diaper change)
   - Nap Times
   - Interactive Engagement (e.g., singing songs, playing games)
   - Safety or Concerning Incidents
   - Disciplinary or Guidance Moments

2. **Markdown Summary Requirements**
   - Use a **formal, informative tone**.
   - Organize the summary under headings for each category (e.g., "## Routine Milestones," "## Meals & Snacks," etc.).
   - Within each heading, use **concise bullet points** (one or two sentences max).
   - **Include timestamps** (e.g., “[07:00:04]”) but **exclude room names**.
   - **Replace any personal names** with generic references (“the nanny,” “the child,” “the mother,” “the father,” “another adult,” etc.) based on context.
   - If the transcript mentions a figurative or playful item (e.g., singing about a spider), clarify it’s a nursery rhyme or playful context.
   - **Do not invent or fabricate** details not in the transcript.
   - If a category has no events, note it with 'No data available.'

3. **Structured JSON Requirements**
   - Produce an **array** of JSON objects, where each object represents a notable event.
   - Each JSON object must have:
       - `"timestamp"` (string)
       - `"room_name"` (string)
       - `"event_type"` (one of the above categories)
       - `"event_description"` (concise, accurate summary)
   - **Replace any personal names** with generic references as noted above.
   - **Do not** invent or fabricate details not in the transcript.
   - If an event covers multiple categories or it's unclear, choose the most appropriate category.
   - If a category has no events, there is no need to add it to the JSON.

4. **General Rules**
   - Remain **factual** to avoid hallucinations.
   - **No mention** of personal names in outputs.
   - If the user requests the markdown summary, provide it under the specified headings in bullet points.
   - If the user requests the JSON output, provide strictly valid JSON with no additional formatting.
   - If the transcript references additional names like “Mama,” “Mommy,” “Daddy,” etc., replace them with “the mother,” “the father,” “another adult,” or a similarly generic reference based on context.

Follow these instructions unless explicitly overridden by the user.
'"####;

pub const SUMMARY_USER_PROMPT: &str = r####"
Please read the following CSV transcript lines and produce a markdown summary of notable events for the day.

Remember to:
- Categorize events by the specified categories (Routine Milestones, Meals & Snacks, Diaper/Potty, Nap Times, Interactive Engagement, Safety or Concerning Incidents, Disciplinary or Guidance Moments).
- Use bullet points, keep them concise, and include timestamps but exclude room names.
- Replace personal names with generic references (e.g., “the nanny,” “the child,” “the mother,” “the father,” “another adult”).
- If there are no events for a given category, you may omit that category.

**CSV Data**:
{transcript}"####;

pub const SUMMARY_USER_PROMPT_JSON: &str = r####"
Now produce a structured JSON list of the notable events from the same CSV.

Remember to include:
- "timestamp"
- "room_name"
- "event_type" (Routine Milestones, Meals & Snacks, Diaper/Potty, Nap Times, Interactive Engagement, Safety or Concerning Incidents, Disciplinary or Guidance Moments)
- "event_description"

Replace any personal names with generic references. Only include events that actually appear in the CSV, and do not invent details.
"####;
