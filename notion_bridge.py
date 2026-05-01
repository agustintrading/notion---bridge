"""
══════════════════════════════════════════════════════════════════════
NOTION BRIDGE — Sync trade dal backtester a Notion con screenshot
══════════════════════════════════════════════════════════════════════
Le credenziali sono lette da variabili d'ambiente (sicure).
Configurale su Railway → Variables.

VARIABILI RICHIESTE:
  NOTION_TOKEN
  DB_WIN_LOSS
  DB_WIN_PERSI
  DB_BE
  CLOUDINARY_CLOUD
  CLOUDINARY_KEY
  CLOUDINARY_SECRET
══════════════════════════════════════════════════════════════════════
"""
import base64
import hashlib
import json
import os
import time
import urllib.request
import urllib.error

from flask import Flask, request, jsonify
from flask_cors import CORS

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
DB_WIN_LOSS  = os.environ.get("DB_WIN_LOSS", "")
DB_WIN_PERSI = os.environ.get("DB_WIN_PERSI", "")
DB_BE        = os.environ.get("DB_BE", "")

CLOUDINARY_CLOUD  = os.environ.get("CLOUDINARY_CLOUD", "")
CLOUDINARY_KEY    = os.environ.get("CLOUDINARY_KEY", "")
CLOUDINARY_SECRET = os.environ.get("CLOUDINARY_SECRET", "")

NOTION_VERSION = "2022-06-28"
PORT = int(os.environ.get("PORT", 5000))

DOW_LABELS = ["Dom", "Lun", "Mar", "Mer", "Gio", "Ven", "Sab"]
MONTH_LABELS_IT = ["", "Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
                   "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]


def upload_to_cloudinary(image_b64, public_id):
    img_bytes = base64.b64decode(image_b64.split(",")[-1])
    ts = str(int(time.time()))
    sig_str = f"public_id={public_id}&timestamp={ts}{CLOUDINARY_SECRET}"
    sig = hashlib.sha1(sig_str.encode()).hexdigest()
    boundary = f"----BridgeBoundary{ts}"

    parts = []
    def field(name, value):
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode()
        )
    field("api_key", CLOUDINARY_KEY)
    field("timestamp", ts)
    field("signature", sig)
    field("public_id", public_id)
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{public_id}.png"\r\n'
        f'Content-Type: image/png\r\n\r\n'.encode()
    )
    parts.append(img_bytes)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    body = b"".join(parts)

    req = urllib.request.Request(
        f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/image/upload",
        data=body, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
        return data["secure_url"]


def notion_request(method, url, body=None):
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_err = e.read().decode()
        raise Exception(f"Notion API {e.code}: {body_err[:500]}")


def ensure_select_option(db_id, prop_name, value):
    try:
        db = notion_request("GET", f"https://api.notion.com/v1/databases/{db_id}")
        prop = db.get("properties", {}).get(prop_name)
        if not prop or "select" not in prop:
            return
        existing = [o["name"] for o in prop["select"].get("options", [])]
        if value in existing:
            return
        new_options = prop["select"]["options"] + [{"name": value}]
        notion_request("PATCH", f"https://api.notion.com/v1/databases/{db_id}", {
            "properties": {prop_name: {"select": {"options": new_options}}}
        })
    except Exception as e:
        print(f"[ensure_select_option] WARN {prop_name}={value}: {e}")


def determine_target_db(trade):
    result = trade.get("result", "").upper()
    condotta = (trade.get("tags", {}) or {}).get("condotta")
    if result == "BE":
        return DB_BE, "BE"
    if result == "WIN":
        if condotta == "perso":
            return DB_WIN_PERSI, "WIN PERSO"
        return DB_WIN_LOSS, "WIN PRESO"
    if result == "LOSS":
        return DB_WIN_LOSS, "LOSS"
    return None, None


def build_notion_properties(trade, target_label, db_id):
    tags = trade.get("tags", {}) or {}
    idmCtx = trade.get("idmCtx") or {}

    entry_ts = trade.get("entryTime")
    import datetime as dt
    d = dt.datetime.fromtimestamp(entry_ts, tz=dt.timezone.utc)
    date_iso = d.strftime("%Y-%m-%d")
    time_str = d.strftime("%H:%M")
    dow = DOW_LABELS[(d.weekday() + 1) % 7]
    mese_label = f"{MONTH_LABELS_IT[d.month]} {d.year}"

    pair_emoji = ""
    if target_label == "WIN PRESO":
        pair_emoji = "✅ "
    elif target_label == "LOSS":
        pair_emoji = "❌ "
    elif target_label == "WIN PERSO":
        pair_emoji = "💔 "
    elif target_label == "BE":
        pair_emoji = "🟰 "
    pair_text = f"{pair_emoji}EURUSD"

    if target_label in ("WIN PRESO", "WIN PERSO"):
        rr_eff = 5.0
    elif target_label == "LOSS":
        rr_eff = -1.0
    elif target_label == "BE":
        rr_eff = float(trade.get("peakR", 0) or 0)
    else:
        rr_eff = 0.0

    idm_type = "External"
    if idmCtx.get("originPrice") and idmCtx.get("extOrgPrice"):
        idm_type = "Internal"

    sweep_pips = idmCtx.get("sweepPip")
    sweep_pips_val = float(sweep_pips) if sweep_pips is not None else None

    extra_parts = []
    for k, v in tags.items():
        if k in ("inducement", "channel", "pipBE", "condotta"):
            continue
        if v in (None, "", False):
            continue
        label = k.replace("ct_", "").replace("_", " ").title()
        extra_parts.append(f"{label}: {v}")
    tags_extra_str = " | ".join(extra_parts) if extra_parts else ""

    ensure_select_option(db_id, "MESE", mese_label)
    if tags.get("channel"):
        ensure_select_option(db_id, "CHANNEL", tags["channel"])

    props = {
        "PAIR": {"title": [{"text": {"content": pair_text}}]},
        "DATA": {"date": {"start": date_iso}},
        "GIORNO": {"select": {"name": dow}},
        "ORA": {"rich_text": [{"text": {"content": time_str}}]},
        "MESE": {"select": {"name": mese_label}},
        "IDM TYPE": {"select": {"name": idm_type}},
        "RISULTATO": {"select": {"name": target_label}},
        "RR EFF.": {"number": rr_eff},
        "SESSIONE": {"select": {"name": trade.get("session", "London")}},
    }
    if tags.get("inducement"):
        props["INDUCEMENT"] = {"select": {"name": tags["inducement"]}}
    if tags.get("channel"):
        props["CHANNEL"] = {"select": {"name": tags["channel"]}}
    if tags.get("condotta"):
        cond_map = {"ok": "OK", "errore": "Errore", "dubbioso": "Dubbioso",
                    "preso": "Preso", "perso": "Perso"}
        props["CONDOTTA"] = {"select": {"name": cond_map.get(tags["condotta"], tags["condotta"])}}
    if tags.get("pipBE"):
        props["1 PIP BE"] = {"select": {"name": tags["pipBE"]}}
    if sweep_pips_val is not None:
        props["SWEEP PIPS"] = {"number": sweep_pips_val}
    note = trade.get("note", "")
    if note:
        props["NOTE"] = {"rich_text": [{"text": {"content": note[:2000]}}]}
    if tags_extra_str:
        props["Tags Extra"] = {"rich_text": [{"text": {"content": tags_extra_str[:2000]}}]}

    return props


def build_page_content(trade, screenshot_url, screenshot2_url=None):
    blocks = []
    info_parts = []
    info_parts.append(f"Trade #{trade.get('num','?')}")
    info_parts.append(f"Dir: {trade.get('dir','?')}")
    info_parts.append(f"Entry: {trade.get('entryPrice', 0):.5f}")
    info_parts.append(f"SL pip: {trade.get('slPip','?')}")
    info_parts.append(f"TP pip: {trade.get('tpPip','?')}")
    info_text = "  ·  ".join(info_parts)

    blocks.append({
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"text": {"content": info_text}, "annotations": {"color": "gray"}}]
        }
    })

    # Screenshot 1: zoom area trade
    if screenshot_url:
        blocks.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": "📍 Zoom area trade"}}]}
        })
        blocks.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": screenshot_url}}
        })

    # Screenshot 2: contesto giornata Asia → 21:00
    if screenshot2_url:
        blocks.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": "🌍 Vista contesto (Asia → 21:00)"}}]}
        })
        blocks.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": screenshot2_url}}
        })

    return blocks


app = Flask(__name__)
CORS(app)


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "ok": True,
        "service": "Notion Bridge",
        "version": "2.2",
        "endpoints": ["/sync", "/update", "/delete", "/report", "/health"]
    })


@app.route("/sync", methods=["POST"])
def sync():
    try:
        payload = request.get_json(force=True)
        trade = payload.get("trade") or {}
        screenshot = payload.get("screenshot")
        screenshot2 = payload.get("screenshot2")  # F8b: secondo screenshot contesto

        if not trade.get("id"):
            return jsonify({"ok": False, "error": "Missing trade.id"}), 400

        db_id, target_label = determine_target_db(trade)
        if db_id is None:
            return jsonify({"ok": False, "error": f"Trade non sincronizzabile (result={trade.get('result')})"}), 400

        # Upload screenshots
        screenshot_url = None
        screenshot2_url = None
        if screenshot:
            try:
                public_id = f"trade_{trade.get('id')}_zoom_{int(time.time())}"
                screenshot_url = upload_to_cloudinary(screenshot, public_id)
            except Exception as e:
                print(f"[sync] WARN screenshot1 fallito: {e}")
        if screenshot2:
            try:
                public_id = f"trade_{trade.get('id')}_ctx_{int(time.time())}"
                screenshot2_url = upload_to_cloudinary(screenshot2, public_id)
            except Exception as e:
                print(f"[sync] WARN screenshot2 fallito: {e}")

        props = build_notion_properties(trade, target_label, db_id)
        children = build_page_content(trade, screenshot_url, screenshot2_url)

        body = {
            "parent": {"database_id": db_id},
            "properties": props,
            "children": children,
        }
        result = notion_request("POST", "https://api.notion.com/v1/pages", body)
        page_id = result.get("id")
        page_url = result.get("url")
        print(f"[sync] OK trade #{trade.get('num')} → {target_label} → {page_url}")

        return jsonify({
            "ok": True,
            "page_id": page_id,
            "page_url": page_url,
            "target": target_label,
            "screenshot_url": screenshot_url,
            "screenshot2_url": screenshot2_url,
        })

    except Exception as e:
        print(f"[sync] ERROR: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/update", methods=["POST"])
def update():
    """Aggiorna una pagina Notion esistente (properties + screenshots)."""
    try:
        payload = request.get_json(force=True)
        trade = payload.get("trade") or {}
        page_id = payload.get("page_id")
        screenshot = payload.get("screenshot")
        screenshot2 = payload.get("screenshot2")

        if not page_id:
            return jsonify({"ok": False, "error": "Missing page_id"}), 400
        if not trade.get("id"):
            return jsonify({"ok": False, "error": "Missing trade.id"}), 400

        db_id, target_label = determine_target_db(trade)
        if db_id is None:
            return jsonify({"ok": False, "error": f"Trade non sincronizzabile (result={trade.get('result')})"}), 400

        # Upload nuovi screenshot
        screenshot_url = None
        screenshot2_url = None
        if screenshot:
            try:
                public_id = f"trade_{trade.get('id')}_zoom_{int(time.time())}"
                screenshot_url = upload_to_cloudinary(screenshot, public_id)
            except Exception as e:
                print(f"[update] WARN screenshot1: {e}")
        if screenshot2:
            try:
                public_id = f"trade_{trade.get('id')}_ctx_{int(time.time())}"
                screenshot2_url = upload_to_cloudinary(screenshot2, public_id)
            except Exception as e:
                print(f"[update] WARN screenshot2: {e}")

        props = build_notion_properties(trade, target_label, db_id)

        # 1. PATCH properties della pagina
        notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", {
            "properties": props
        })

        # 2. Cancella tutti i children esistenti e aggiungi nuovi
        try:
            existing_children = notion_request("GET", f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=50")
            for block in existing_children.get("results", []):
                bid = block.get("id")
                if bid:
                    try:
                        notion_request("DELETE", f"https://api.notion.com/v1/blocks/{bid}")
                    except Exception as e:
                        print(f"[update] WARN delete block {bid}: {e}")
        except Exception as e:
            print(f"[update] WARN list children: {e}")

        # 3. Aggiungi i nuovi children
        new_children = build_page_content(trade, screenshot_url, screenshot2_url)
        if new_children:
            notion_request("PATCH", f"https://api.notion.com/v1/blocks/{page_id}/children", {
                "children": new_children
            })

        page_url = f"https://www.notion.so/{page_id.replace('-','')}"
        print(f"[update] OK trade #{trade.get('num')} → {target_label} → {page_url}")

        return jsonify({
            "ok": True,
            "page_id": page_id,
            "page_url": page_url,
            "target": target_label,
            "screenshot_url": screenshot_url,
            "screenshot2_url": screenshot2_url,
        })

    except Exception as e:
        print(f"[update] ERROR: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/delete", methods=["POST"])
def delete():
    """Archivia una pagina Notion (cancellazione soft)."""
    try:
        payload = request.get_json(force=True)
        page_id = payload.get("page_id")
        if not page_id:
            return jsonify({"ok": False, "error": "Missing page_id"}), 400
        notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", {
            "archived": True
        })
        print(f"[delete] OK page {page_id} archiviata")
        return jsonify({"ok": True, "page_id": page_id, "archived": True})
    except Exception as e:
        print(f"[delete] ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/report", methods=["POST"])
def create_report():
    """Crea pagina report mensile sul DB WIN+LOSS."""
    try:
        payload = request.get_json(force=True)
        title = payload.get('title', 'Report Mensile')
        date_iso = payload.get('date')
        blocks = payload.get('blocks', [])
        equity_real_b64 = payload.get('equity_real')
        equity_strat_b64 = payload.get('equity_strategy')
        report_preview_b64 = payload.get('report_preview')  # F19 v2.2: nuovo

        # Upload immagini Cloudinary
        url_real = None
        url_strat = None
        url_preview = None
        ts = int(time.time())
        if equity_real_b64:
            try:
                url_real = upload_to_cloudinary(equity_real_b64, f"report_real_{ts}")
            except Exception as e:
                print(f"[report] WARN equity_real: {e}")
        if equity_strat_b64:
            try:
                url_strat = upload_to_cloudinary(equity_strat_b64, f"report_strat_{ts}")
            except Exception as e:
                print(f"[report] WARN equity_strat: {e}")
        if report_preview_b64:
            try:
                url_preview = upload_to_cloudinary(report_preview_b64, f"report_preview_{ts}")
            except Exception as e:
                print(f"[report] WARN preview: {e}")

        # Sostituisci placeholder nei blocchi
        for blk in blocks:
            if blk.get('type') == 'image' and 'placeholder' in blk.get('image', {}):
                ph = blk['image']['placeholder']
                if ph == 'EQUITY_REAL' and url_real:
                    blk['image'] = {'type': 'external', 'external': {'url': url_real}}
                elif ph == 'EQUITY_STRATEGY' and url_strat:
                    blk['image'] = {'type': 'external', 'external': {'url': url_strat}}
                elif ph == 'REPORT_PREVIEW' and url_preview:
                    blk['image'] = {'type': 'external', 'external': {'url': url_preview}}
                else:
                    blk['type'] = 'paragraph'
                    blk['paragraph'] = {'rich_text': [{'text': {'content': '[immagine non disponibile]'}}]}
                    del blk['image']

        # Crea pagina nel DB
        body = {
            "parent": {"database_id": DB_WIN_LOSS},
            "icon": {"type": "emoji", "emoji": "✏️"},
            "properties": {
                "PAIR": {"title": [{"text": {"content": title}}]},
            },
            "children": blocks
        }
        if date_iso:
            body["properties"]["DATA"] = {"date": {"start": date_iso}}

        result = notion_request("POST", "https://api.notion.com/v1/pages", body)
        print(f"[report] OK {title} → {result.get('url')}")
        return jsonify({
            "ok": True,
            "page_id": result.get('id'),
            "page_url": result.get('url'),
            "equity_real_url": url_real,
            "equity_strat_url": url_strat,
        })

    except Exception as e:
        print(f"[report] ERROR: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    out = {"notion": False, "cloudinary": False}
    try:
        notion_request("GET", f"https://api.notion.com/v1/databases/{DB_WIN_LOSS}")
        out["notion"] = True
    except Exception as e:
        out["notion_error"] = str(e)
    try:
        req = urllib.request.Request(
            f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/usage",
            headers={"Authorization": "Basic " + base64.b64encode(
                f"{CLOUDINARY_KEY}:{CLOUDINARY_SECRET}".encode()).decode()}
        )
        urllib.request.urlopen(req, timeout=8).read()
        out["cloudinary"] = True
    except Exception as e:
        out["cloudinary_error"] = str(e)
    return jsonify(out)


if __name__ == "__main__":
    print(f"Notion Bridge starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
