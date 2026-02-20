# Summarizer
Runs as https://obedio.ai
### Parameters are:
- summarize a pdf from GCS
  /s?file=[directory]/filename.pdf
### API Endpoints
| Route | Method | Parameter                | Description                                                                                  |
| :---- | :----- | :----------------------- | -------------------------------------------------------------------------------------------- |
| `/s`  | GET    | `file`                   | Summarizes a specific PDF stored in Google Cloud Storage. Requires full path.                |
| `/g`  | GET    | `file`                   | Fetches and serves a raw PDF file from the `GCS/meetings` bucket/folder.                     |
| `/q`  | GET    | `code`, `topic`, `theme` | Queries the `meetings_detail` table. Results in Datatables-compatible JSON.                  |
| `/j`  | GET    | `state`, `jtype`         | Queries `jurisdictions`. `state` is 'all' or CSV. `jtype` is 'M' (Municipal) or 'S' (State). |
### Usage Examples:
**Querying Meetings:**
`GET /q?topic=zoning&theme=development`
**Filtering Jurisdictions:**
`GET /j?state=CA,NY,TX&jtype=M`
