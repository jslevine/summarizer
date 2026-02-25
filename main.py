#!/usr/bin/env python3
import functions_framework
from google import genai
from google.genai import types
from google.cloud import storage
from google.cloud import bigquery
import pypdf
import io
import json
import datetime
import decimal
# --- Configuration ---
PROJECT_ID = "obedio"
LOCATION = "us-central1"
BUCKET_NAME = "obedio.appspot.com"
# Limits
MAX_FILE_SIZE_MB = 300
TRUNCATE_LIMIT = 50
FALLBACK_PAGES = 25
# --- Initialize Services (Global Scope for Warm Start) ---
_genai_client = None
_storage_client = None
_bq_client = None
def get_genai_client():
    global _genai_client
    if _genai_client is None:
        _genai_client = genai.Client(
            vertexai=True, project=PROJECT_ID, location=LOCATION
        )
    return _genai_client
def get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project=PROJECT_ID)
    return _storage_client
def get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client
@functions_framework.http
def main_router(request):
    path = request.path.rstrip("/")
    if path == "":
        print("Health check request received", flush=True)
        return "OK", 200
    if path == "/g":
        return get_file(request)
    elif path == "/s":
        return summarize_pdf(request)
    elif path == "/q":
        return query_bigquery_charts(request)
    elif path == "/j":  # <--- Add this
        return query_bigquery_json(request)
    else:
        if request.method == "OPTIONS":
            return handle_cors_options()
        return "Invalid route. Use /g, /s, /q, or /j.", 404
# ---------------------------------------------------------
# ROUTE: /g (Get File)
# ---------------------------------------------------------
def get_file(request):
    """Reads a file from GCS/meetings and returns it raw."""
    request_json = request.get_json(silent=True)
    request_args = request.args
    request_form = request.form
    filename = (
        request_args.get("file")
        or (request_json and request_json.get("file"))
        or (request_form and request_form.get("file"))
    )
    if not filename:
        return 'Error: Missing "file" param.', 400
    try:
        bucket = get_storage_client().bucket(BUCKET_NAME)
        # Handle "meetings/" prefix
        blob_path = (
            filename if filename.startswith("meetings/") else f"meetings/{filename}"
        )
        blob = bucket.blob(blob_path)
        if not blob.exists():
            return "Error: File not found.", 404
        data = blob.download_as_bytes()
        content_type = blob.content_type or "application/octet-stream"
        return data, 200, {"Content-Type": content_type}
    except Exception as e:
        return f"Error reading file: {str(e)}", 500
# ---------------------------------------------------------
# ROUTE: /s (Summarize PDF)
# ---------------------------------------------------------
def summarize_pdf(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    request_form = request.form
    filename = (
        request_args.get("file")
        or (request_json and request_json.get("file"))
        or (request_form and request_form.get("file"))
    )
    topic = (
        request_args.get("topic")
        or (request_json and request_json.get("topic"))
        or (request_form and request_form.get("topic"))
    )
    theme = (
        request_args.get("theme")
        or (request_json and request_json.get("theme"))
        or (request_form and request_form.get("theme"))
    )
    if not filename:
        return 'Error: Missing "file" param.', 400
    # Build instructions
    focus_phrase = ""
    if topic and theme:
        focus_phrase = f", focused on the topic of '{topic}' and the theme of '{theme}'"
    elif topic:
        focus_phrase = f", focused on the topic of '{topic}'"
    elif theme:
        focus_phrase = f", focused on the theme of '{theme}'"
    default_instr = (
        prompt
    ) = f"""
ACT AS A RAW HTML GENERATOR. 
Output ONLY valid HTML code. No talk. No Markdown. NO BACKTICKS (```).
Summarize this document {focus_phrase}.
### MANDATORY URL PROTOCOL:
You MUST monitor for URLs at all times. If the source text mentions a website or link (e.g., [https://example.com](https://example.com)), you MUST include it immediately.
- **Correct Format:** "For more info, visit <a href='[https://obedio.ai](https://obedio.ai)' style='color: #0066cc; text-decoration: underline;'>[https://obedio.ai](https://obedio.ai)</a>."
- **Constraint:** Never remove a URL to save space. Never paraphrase a URL.
### MANDATORY HTML SKELETON:
You MUST structure the output exactly like this:
<div style="max-width: 800px; margin: 20px auto; padding: 5% 7%; font-family: sans-serif; line-height: 1.6; color: #333; background-color: #fff; border: 1px solid #e0e0e0; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
  <div style="margin-bottom: 25px; border-bottom: 2px solid #f0f0f0; padding-bottom: 15px;">
    <img src="[https://cdn.prod.website-files.com/66525ec138843cc1ccb1c325/67de3f1325f71aac6e910217_obedio%20gold%20for%20white%20background%20copy.png](https://cdn.prod.website-files.com/66525ec138843cc1ccb1c325/67de3f1325f71aac6e910217_obedio%20gold%20for%20white%20background%20copy.png)" style="height: 40px;">
  </div>
  [Summary Content Here using <p>, <strong>, and <ul> tags]
  <p style="margin-top: 30px; border-top: 1px solid #eee; padding-top: 20px; color: gray;">
    <small>Disclaimer: This document was generated by Obedio AI from up to the first 50 pages of the original source document. While we strive for accuracy, please verify critical details with the original source document. AI can be wildly inaccurate.</small>
  </p>
</div>
### CRITICAL RULES:
1. **NO ASTERISKS:** Use <strong>bold text</strong>. Never use **bold text**.
2. **URL ENFORCEMENT:** Every URL found MUST be clickable: <a href="URL" style="color: #0066cc; text-decoration: underline;">URL</a>. Do not shorten them.
3. **CHARACTER CLEANING:** You MUST replace 'â€“' with '-', 'â€™' with "'", and 'â€œ' with '"'.
4. **CONTACT INFO:** Create a specific <h2>Related Contact Information</h2> section inside the card.
"""
    custom_instructions = (
        request_args.get("instructions")
        or (request_json and request_json.get("instructions"))
        or (request_form and request_form.get("instructions"))
        or default_instr
    )
    try:
        bucket = get_storage_client().bucket(BUCKET_NAME)
        # Handle "meetings/" prefix
        blob_path = (
            filename if filename.startswith("meetings/") else f"meetings/{filename}"
        )
        blob = bucket.blob(blob_path)
        if not blob.exists():
            return f"Error: File '{filename}' not found.", 404
        blob.reload()
        file_size_mb = blob.size / (1024 * 1024)
        if file_size_mb > MAX_FILE_SIZE_MB:
            return (
                f"Error: File is {file_size_mb:.2f}MB. Limit is {MAX_FILE_SIZE_MB}MB.",
                400,
            )
        pdf_bytes = blob.download_as_bytes()
        try:
            input_stream = io.BytesIO(pdf_bytes)
            pdf_reader = pypdf.PdfReader(input_stream)
            num_pages = len(pdf_reader.pages)
        except Exception as e:
            return f"Error reading PDF structure: {str(e)}", 400
        parts = []
        if num_pages > TRUNCATE_LIMIT or file_size_mb > 30:
            output_stream = io.BytesIO()
            pdf_writer = pypdf.PdfWriter()
            limit = min(FALLBACK_PAGES, num_pages)
            for i in range(limit):
                pdf_writer.add_page(pdf_reader.pages[i])
            pdf_writer.write(output_stream)
            truncated_bytes = output_stream.getvalue()
            parts.append(
                types.Part.from_bytes(data=truncated_bytes, mime_type="application/pdf")
            )
            custom_instructions += (
                f" [NOTE: File truncated. You are seeing the first {limit} pages.]"
            )
        else:
            file_uri = f"gs://{BUCKET_NAME}/{blob_path}"
            parts.append(
                types.Part.from_uri(file_uri=file_uri, mime_type="application/pdf")
            )
        parts.append(types.Part.from_text(text=custom_instructions))
        response = get_genai_client().models.generate_content(
            model="gemini-2.0-flash",
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                temperature=0.0,
                max_output_tokens=1500,  # Prevents the model from "running forever"
                candidate_count=1,
            ),
        )
        if not response.text:
            return (
                f"Error: AI returned empty text. Reason: {response.candidates[0].finish_reason}",
                500,
            )
        cleaned_text = response.text.replace("```html", "").replace("```", "")
        if "<!DOCTYPE" in cleaned_text:
            cleaned_text = cleaned_text[cleaned_text.find("<!DOCTYPE") :]
        elif "<html" in cleaned_text:
            cleaned_text = cleaned_text[cleaned_text.find("<html") :]
        return cleaned_text, 200, {"Content-Type": "text/html"}
    except Exception as e:
        return f"Error processing file: {str(e)}", 500
# ---------------------------------------------------------
# ROUTE: /q (BigQuery Charts API)
# ---------------------------------------------------------
def query_bigquery_charts(request):
    """Queries BigQuery and returns data formatted for Google Charts."""
    if request.method == "OPTIONS":
        return handle_cors_options()
    headers = {"Access-Control-Allow-Origin": "*"}
    request_json = request.get_json(silent=True)
    request_args = request.args
    code_param = request_args.get("code") or (request_json and request_json.get("code"))
    topic_param = request_args.get("topic") or (
        request_json and request_json.get("topic")
    )
    theme_param = request_args.get("theme") or (
        request_json and request_json.get("theme")
    )
    state_param = request_args.get("state") or (
        request_json and request_json.get("state")
    )
    find_param = request_args.get("find") or (request_json and request_json.get("find"))
    from_param = request_args.get("from") or (request_json and request_json.get("from"))
    to_param = request_args.get("to") or (request_json and request_json.get("to"))
    dd_mode = "dd" in request_args
    un_mode = "un" in request_args
    if un_mode:
        try:
            topics_query = "SELECT DISTINCT topic FROM `obedio.meetings.meeting_details` WHERE topic IS NOT NULL ORDER BY topic"
            topics_job = get_bq_client().query(topics_query)
            topics = [row[0] for row in topics_job.result()]
            return json.dumps(topics), 200, headers
        except Exception as e:
            return json.dumps({"error": str(e)}), 500, headers
    sql = "SELECT * FROM `obedio.meetings.meeting_details` AS t"
    where_clauses = []
    query_params = []
    if find_param:
        where_clauses.append("CONTAINS_SUBSTR(t, @find)")
        query_params.append(bigquery.ScalarQueryParameter("find", "STRING", find_param))
    if code_param:
        processed_codes = []
        for raw_code in code_param.split(","):
            c = raw_code.strip()
            if not c:
                continue
            if c[0].upper() in ("M", "S"):
                processed_codes.append(c)
            else:
                processed_codes.append(f"M{c.zfill(8)}")
        if processed_codes:
            where_clauses.append("jcode IN UNNEST(@codes)")
            query_params.append(
                bigquery.ArrayQueryParameter("codes", "STRING", processed_codes)
            )
    if topic_param:
        where_clauses.append("LOWER(topic) LIKE @topic")
        query_params.append(
            bigquery.ScalarQueryParameter("topic", "STRING", f"%{topic_param.lower()}%")
        )
    if theme_param:
        where_clauses.append("LOWER(theme) LIKE @theme")
        query_params.append(
            bigquery.ScalarQueryParameter("theme", "STRING", f"%{theme_param.lower()}%")
        )
    if state_param and state_param.lower() != "all":
        states = [s.strip().upper() for s in state_param.split(",")]
        if states:
            where_clauses.append("state IN UNNEST(@states)")
            query_params.append(
                bigquery.ArrayQueryParameter("states", "STRING", states)
            )
    if from_param:
        where_clauses.append("date >= @from_date")
        query_params.append(
            bigquery.ScalarQueryParameter("from_date", "DATE", from_param)
        )
    if to_param:
        where_clauses.append("date <= @to_date")
        query_params.append(bigquery.ScalarQueryParameter("to_date", "DATE", to_param))
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY date DESC"
    sql += " LIMIT 0" if dd_mode else " LIMIT 5000"
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_params, use_legacy_sql=False
        )
        query_job = get_bq_client().query(sql, job_config=job_config)
        rows = query_job.result()
        schema = rows.schema
        cols = [
            {
                "id": field.name,
                "label": field.name,
                "type": map_bq_type_to_charts(field.field_type),
            }
            for field in schema
        ]
        chart_rows = []
        for row in rows:
            cells = []
            for field in schema:
                val = row[field.name]
                formatted_val = format_value_for_charts(val, field.field_type)
                cells.append({"v": formatted_val})
            chart_rows.append({"c": cells})
        data_table = {"cols": cols, "rows": chart_rows}
        return json.dumps(data_table), 200, headers
    except Exception as e:
        print(f"BQ Error: {e}")
        return json.dumps({"error": str(e)}), 500, headers
# ---------------------------------------------------------
# ROUTE: /j (JSON Data Export)
# ---------------------------------------------------------
def query_bigquery_json(request):
    """Returns matching rows as a standard JSON array of objects."""
    if request.method == "OPTIONS":
        return handle_cors_options()
    headers = {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}
    # 1. Parse parameters
    state_param = request.args.get("state")
    jtype_param = request.args.get("jtype")
    # 2. Build SQL
    sql = "SELECT * FROM `obedio.meetings.jurisdictions`"
    query_params = []
    where_clauses = []
    # Filter by State
    if state_param and state_param.lower() != "all":
        # Clean up input: "ca, ny " -> ["CA", "NY"]
        states = [s.strip().upper() for s in state_param.split(",")]
        if states:
            where_clauses.append("state IN UNNEST(@states)")
            query_params.append(
                bigquery.ArrayQueryParameter("states", "STRING", states)
            )
    # Filter by Type (M or S)
    if jtype_param:
        jtype_val = jtype_param.strip().upper()
        if jtype_val in ("M", "S"):
            where_clauses.append("jtype = @jtype")
            query_params.append(
                bigquery.ScalarQueryParameter("jtype", "STRING", jtype_val)
            )
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    # 3. Execute
    try:
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = get_bq_client().query(sql, job_config=job_config)
        rows = query_job.result()
        # 4. Convert to list of dicts (BigQuery rows are Row objects)
        # We use a custom encoder or manual conversion for Date/Decimal types
        results = []
        for row in rows:
            dict_row = dict(row.items())
            # Convert non-serializable types to strings/floats
            for key, value in dict_row.items():
                if isinstance(value, (datetime.date, datetime.datetime)):
                    dict_row[key] = value.isoformat()
                elif isinstance(value, decimal.Decimal):
                    dict_row[key] = float(value)
            results.append(dict_row)
        return json.dumps(results), 200, headers
    except Exception as e:
        print(f"JSON Route Error: {e}")
        return json.dumps({"error": str(e)}), 500, headers
# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def handle_cors_options():
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "3600",
    }
    return "", 204, headers
def map_bq_type_to_charts(bq_type):
    t = bq_type.upper()
    if t in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
        return "number"
    elif t in ("BOOLEAN", "BOOL"):
        return "boolean"
    elif t == "DATE":
        return "date"
    elif t in ("DATETIME", "TIMESTAMP"):
        return "datetime"
    return "string"
def format_value_for_charts(val, bq_type):
    if val is None:
        return None
    t = bq_type.upper()
    if t == "DATE":
        # val is datetime.date. Month is 0-indexed in JS.
        return f"Date({val.year}, {val.month - 1}, {val.day})"
    elif t in ("DATETIME", "TIMESTAMP"):
        # val is datetime.datetime
        return f"Date({val.year}, {val.month - 1}, {val.day}, {val.hour}, {val.minute}, {val.second})"
    elif t in ("NUMERIC", "BIGNUMERIC"):
        return float(val)
    return val
