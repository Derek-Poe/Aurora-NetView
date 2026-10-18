"""Self-contained static dashboard; untrusted labels are escaped."""

import html


def render(report):
    rows = []
    for client in report["clients"]:
        domains = ", ".join(f"{d['domain']} ({d['hits']})" for d in client["domains"])
        status = "Needs refresh" if client["stale"] else "Fresh"
        rows.append(
            "<tr><td>"
            + html.escape(client["client"])
            + "</td><td>"
            + str(client["queries"])
            + "</td><td>"
            + status
            + "</td><td>"
            + html.escape(domains)
            + "</td></tr>"
        )
    origin = (
        "Synthetic fixture only. No live DNS queries or external assets."
        if report.get("synthetic")
        else "Counts from the configured source. No external assets."
    )
    catchup = (
        "Caught up at last poll" if report["caught_up_at_poll"] else "Catch-up pending"
    )
    return (
        """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Aurora NetView | DNS telemetry</title><style>
body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 20px;background:#111827;color:#e5e7eb}h1{color:#67e8f9}p{line-height:1.6}.cards{display:flex;gap:20px;flex-wrap:wrap}.card{padding:20px;background:#1f2937;border-radius:12px}.card strong{display:block;font-size:30px;color:#67e8f9}table{width:100%;border-collapse:collapse}th,td{padding:12px;text-align:left;border-bottom:1px solid #374151}td:last-child{max-width:460px;overflow-wrap:anywhere}.table{overflow:auto}small{color:#a5b4fc}</style>
<h1>Aurora NetView</h1><p>DNS events &rarr; bounded ingestion &rarr; atomic cursor and four hourly rollups &rarr; fair cached client summaries.</p>
<small>"""
        + origin
        + """</small>
<div class="cards"><div class="card"><strong>"""
        + str(report["queries"])
        + """</strong>Retained queries</div><div class="card"><strong>"""
        + str(report["cursor"])
        + """</strong>Committed source ID</div><div class="card"><strong>"""
        + str(report["poll_age_seconds"])
        + """ s</strong>Poll age at capture</div></div>
<p>"""
        + catchup
        + """; observed source ID """
        + str(report["observed_max_id"])
        + """.
Last successful poll: epoch """
        + str(report["as_of"])
        + """. The window starts on an hour boundary """
        + str(report["retention_hours"])
        + """ hours earlier. This static file does not refresh automatically.</p>
<div class="table"><table><thead><tr><th>Client</th><th>Queries</th><th>Snapshot</th><th>Top domains</th></tr></thead><tbody>"""
        + "".join(rows)
        + """</tbody></table></div></html>"""
    )
