import os
import json
import base64
import uuid
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# ---- Optional: EXIF (레이어 없으면 자동 무시)
try:
    from PIL import Image, ExifTags
    from PIL.ExifTags import TAGS, GPSTAGS
except Exception:
    Image = None

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))

# ---- ENV
MISSIONS_LIVE_TABLE      = os.getenv("MISSIONS_LIVE_TABLE", "Missions_Live")
MISSION_PROGRESS_TABLE   = os.getenv("MISSION_PROGRESS_TABLE", "MissionProgress")

SEOUL_GEO_BUCKET         = os.getenv("SEOUL_GEO_BUCKET")          # ex) halsaram-geo
SEOUL_GEO_KEY            = os.getenv("SEOUL_GEO_KEY")             # ex) boundaries/seoul_municipalities_geo_simple.json
DISTRICT_NAME            = os.getenv("DISTRICT_NAME", "Songpa-gu")

PROMPT_BUCKET            = os.getenv("PROMPTS_BUCKET", "halsaram-prompts")
PROCESS_PROMPTS_PREFIX   = os.getenv("PROCESS_PROMPTS_PREFIX", "processPrompts/")

# 포인트 환경변수 제거

missions_tbl = dynamodb.Table(MISSIONS_LIVE_TABLE)
progress_tbl = dynamodb.Table(MISSION_PROGRESS_TABLE)

# ---------- utils ----------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_object_bytes(bucket, key):
    print("[DEBUG] get_object try:", bucket, "|", repr(key))
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read()
    content_type = obj.get("ContentType")
    meta = {k.lower(): v for k, v in obj.get("Metadata", {}).items()}
    last_modified = obj.get("LastModified")  # tz-aware datetime
    return content, content_type, meta, last_modified

def guess_media_type(key, content_type):
    if content_type:
        return content_type
    l = key.lower()
    if l.endswith(".png"): return "image/png"
    if l.endswith(".webp"): return "image/webp"
    return "image/jpeg"

def parse_ids_from_meta_or_key(key, meta):
    mission_id = meta.get("missionid") or meta.get("mission_id")
    user_id    = meta.get("userid") or meta.get("user_id")
    step_index = meta.get("stepindex") or meta.get("step_index")
    if step_index is not None:
        try: step_index = int(step_index)
        except: step_index = None
    if not mission_id or not user_id or step_index is None:
        parts = key.split("/")
        if len(parts) >= 5:
            mission_id = mission_id or parts[-4]
            user_id    = user_id    or parts[-3]
            try:
                step_index = step_index if step_index is not None else int(parts[-2])
            except:
                pass
    return mission_id, user_id, step_index

# ---- Decimal 변환
def _to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, int):
        return Decimal(int(obj))
    if isinstance(obj, dict):
        return {k: _to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_decimal(v) for v in obj]
    return obj

# ---- EXIF / GPS
def _rat_to_float(v):
    try:
        return float(v.n) / float(v.d)
    except Exception:
        if isinstance(v, (list, tuple)) and len(v) == 2:
            return float(v[0]) / float(v[1])
        return float(v)

def dms_to_decimal(dms, ref):
    deg = _rat_to_float(dms[0])
    minutes = _rat_to_float(dms[1])
    seconds = _rat_to_float(dms[2])
    val = deg + minutes/60.0 + seconds/3600.0
    return -val if ref in ["S", "W"] else val

def extract_gps_from_exif(tmp_path):
    if Image is None:
        print("[EXIF] PIL available: False")
        return None
    print("[EXIF] PIL available: True")
    try:
        img = Image.open(tmp_path)
        exif = img._getexif()
        if not exif:
            return None
        exif_data = {TAGS.get(t, t): v for t, v in exif.items()}
        gps = exif_data.get("GPSInfo")
        if not gps:
            return None
        g = {GPSTAGS.get(k, k): v for k, v in gps.items()}
        print("[EXIF] raw GPSInfo keys:", list(g.keys()))
        if "GPSLatitude" in g and "GPSLongitude" in g and "GPSLatitudeRef" in g and "GPSLongitudeRef" in g:
            lat = dms_to_decimal(g["GPSLatitude"], g["GPSLatitudeRef"])
            lon = dms_to_decimal(g["GPSLongitude"], g["GPSLongitudeRef"])
            print("[EXIF] parsed lat/lon:", lat, lon, "ref:", g.get("GPSLatitudeRef"), g.get("GPSLongitudeRef"))
            return {"lat": lat, "lon": lon}
    except Exception as e:
        print("[EXIF][ERROR]", repr(e))
        return None
    return None

def extract_exif_datetime_epoch(data_bytes):
    if Image is None:
        return None
    try:
        tmp = f"/tmp/{uuid.uuid4().hex}"
        with open(tmp, "wb") as f:
            f.write(data_bytes)
        img = Image.open(tmp)
        ex = img._getexif()
        if not ex: return None
        TAGS_REV = {v: k for k, v in ExifTags.TAGS.items()}
        dto_key = TAGS_REV.get("DateTimeOriginal")
        if not dto_key or dto_key not in ex:
            return None
        raw = ex[dto_key]  # "YYYY:MM:DD HH:MM:SS"
        exif_dt = datetime.strptime(raw, "%Y:%m:%d %H:%M:%S")
        return int(exif_dt.timestamp())
    except Exception:
        return None

def fetch_mission_flat(mission_id):
    res = missions_tbl.get_item(Key={"mission_id": mission_id})
    item = res.get("Item")
    if not item:
        return None, None
    mission = {
        "name":         item.get("name"),
        "steps":        item.get("steps") or [],
        "difficulty":   int(item.get("difficulty", 1)),
        "participants": int(item.get("participants", 3)),
    }
    return item, mission

# ---- GeoJSON: district PIP (멀티폴리곤/홀 지원)
_district_polys = None  # List[List[Ring]]

def _load_seoul_geojson():
    if not SEOUL_GEO_BUCKET or not SEOUL_GEO_KEY:
        raise RuntimeError("Seoul GeoJSON env vars not set")
    print("[GEO] try get_object: bucket={}, key={}".format(SEOUL_GEO_BUCKET, SEOUL_GEO_KEY))
    obj = s3.get_object(Bucket=SEOUL_GEO_BUCKET, Key=SEOUL_GEO_KEY)
    return json.loads(obj["Body"].read().decode("utf-8"))

def _feature_matches_district(props, want):
    if not isinstance(props, dict):
        return False
    want_norm = str(want).strip().lower()
    candidate_keys = ["SIG_KOR_NM","SIG_ENG_NM","name","NAME","NAME_1","adm_nm","admName","gu","GUNAME"]
    for k, v in props.items():
        if k in candidate_keys and isinstance(v, str):
            if v.strip().lower() == want_norm:
                return True
    kor_eng = {"송파구": "songpa-gu", "songpa-gu": "송파구"}
    alt = kor_eng.get(want_norm)
    if alt:
        for k, v in props.items():
            if k in candidate_keys and isinstance(v, str):
                if v.strip().lower() == alt:
                    return True
    return False

def _load_district_polygon_from_seoul():
    global _district_polys
    if _district_polys is not None:
        return
    gj = _load_seoul_geojson()
    feats = gj.get("features", []) if gj.get("type") == "FeatureCollection" else [gj]

    geom = None
    for feat in feats:
        if _feature_matches_district(feat.get("properties", {}), DISTRICT_NAME):
            geom = feat.get("geometry"); break
    if not geom:
        for cand in ("송파구", "Songpa-gu"):
            for feat in feats:
                if _feature_matches_district(feat.get("properties", {}), cand):
                    geom = feat.get("geometry"); break
            if geom: break
    if not geom:
        raise RuntimeError(f"District '{DISTRICT_NAME}' not found")

    polys = []
    if geom.get("type") == "Polygon":
        rings = []
        for ring in geom["coordinates"]:
            rings.append([(float(x), float(y)) for x, y in ring])  # (lon, lat)
        polys.append(rings)
    elif geom.get("type") == "MultiPolygon":
        for poly in geom["coordinates"]:
            rings = []
            for ring in poly:
                rings.append([(float(x), float(y)) for x, y in ring])
            polys.append(rings)
    else:
        raise RuntimeError("Geometry must be Polygon or MultiPolygon")

    _district_polys = polys

def _point_in_ring(point, ring):
    x, y = point
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]; x2, y2 = ring[(i+1) % n]
        if ((y1 > y) != (y2 > y)):
            xinters = (x2 - x1) * (y - y1) / (y2 - y1 + 1e-15) + x1
            if xinters >= x:  # 경계 포함
                inside = not inside
    return inside

def _point_in_polygon_with_holes(point, rings):
    if not rings:
        return False
    if not _point_in_ring(point, rings[0]):
        return False
    for hole in rings[1:]:
        if _point_in_ring(point, hole):
            return False
    return True

def is_within_district(gps):
    if not gps:
        return False
    _load_district_polygon_from_seoul()
    pt = (float(gps["lon"]), float(gps["lat"]))  # (lon, lat)
    for rings in _district_polys:
        if _point_in_polygon_with_holes(pt, rings):
            return True
    return False

# ---- S3 prompt loader: 최신 파일 자동 선택
def _get_latest_key(bucket: str, prefix: str) -> str:
    continuation = None
    latest = None
    while True:
        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if continuation:
            kwargs['ContinuationToken'] = continuation
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue
            if latest is None or obj['LastModified'] > latest['LastModified']:
                latest = obj
        if not resp.get('IsTruncated'):
            break
        continuation = resp.get('NextContinuationToken')
    if not latest:
        raise FileNotFoundError(f'No process prompt under s3://{bucket}/{prefix}')
    return latest['Key']

_prompt_cfg_cache = None
def load_process_prompt():
    """
    S3의 최신 process_prompt.json을 읽어 policy/judge_instructions/user_prompt_template 기반으로
    최종 prompt_template을 합성해 반환.
    """
    global _prompt_cfg_cache
    if _prompt_cfg_cache is not None:
        return _prompt_cfg_cache

    # 최신 키 선택
    key = _get_latest_key(PROMPT_BUCKET, PROCESS_PROMPTS_PREFIX)
    obj = s3.get_object(Bucket=PROMPT_BUCKET, Key=key)
    cfg = json.loads(obj["Body"].read().decode("utf-8"))
    cfg["_resolved_key"] = key

    # 필수 기본값
    cfg.setdefault("model_id", "anthropic.claude-3-haiku-20240307-v1:0")
    cfg.setdefault("confidence_threshold", 0.55)

    # 기존 prompt_template가 있으면 그대로 사용, 없으면 policy/judge_instructions로 합성
    if "prompt_template" not in cfg:
        lang = (cfg.get("policy", {}) or {}).get("language", "ko")
        schema = (cfg.get("policy", {}) or {}).get("schema", {})
        judge_lines = cfg.get("judge_instructions", []) or []
        user_tpl = cfg.get("user_prompt_template", "단계 설명: {step_text}")

        judge_txt = "\n".join(judge_lines)
        # 출력 스키마 안내(참고용)
        schema_hint = json.dumps(schema, ensure_ascii=False)

        prompt_template = (
            f"[언어] {lang}\n"
            f"{judge_txt}\n\n"
            f"{user_tpl}\n\n"
            f"[출력 형식]\n"
            f"- 반드시 JSON 한 줄(single_line_json)\n"
            f"- 스키마 예시: {schema_hint}"
        )
        cfg["prompt_template"] = prompt_template

    _prompt_cfg_cache = cfg
    return cfg

def build_vision_prompt(step_text, cfg):
    # user_prompt_template에 {step_text} 치환 → 그 결과를 포함하는 최종 prompt_template 생성
    tpl = cfg.get("prompt_template", "")
    # 안전하게 한 번 더 치환
    tpl = tpl.replace("{step_text}", step_text if step_text else "")
    return tpl

def ask_bedrock_vision(model_id, prompt_text, image_b64, media_type):
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt_text},
                {"type": "image",
                 "source": {"type": "base64", "media_type": media_type, "data": image_b64}}
            ]
        }]
    }
    resp = bedrock.invoke_model(modelId=model_id, body=json.dumps(body).encode("utf-8"))
    payload = json.loads(resp["body"].read())
    texts = [c.get("text","") for c in payload.get("content", []) if c.get("type")=="text"]
    raw = "\n".join(texts).strip()
    try:
        return json.loads(raw)
    except Exception:
        import re
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try: return json.loads(m.group(0))
            except: pass
    return {"match": False, "confidence": 0.0, "reasons": "모델 응답 파싱 실패"}

# ---- Write per-photo log item ----
def put_progress_log(mission_id, user_id, step_index, status, details):
    sk = f"{user_id}#{now_iso()}"
    item = {
        "mission_id": mission_id,
        "user_id_ts": sk,
        "user_id": user_id,
        "step_index": int(step_index) if isinstance(step_index, int) else -1,
        "status": status,
        "details": _to_decimal(details),
        "created_at": Decimal(str(time.time()))
    }
    progress_tbl.put_item(Item=item)
    return item

# ---- Aggregate update (atomic) ----
def update_aggregate_on_approve(mission_id, user_id, step_index, total_steps):
    try:
        resp = progress_tbl.update_item(
            Key={"mission_id": mission_id, "user_id_ts": f"agg#{user_id}"},
            UpdateExpression=(
                "ADD approved_steps :s, approved_count :one "
                "SET total_steps = if_not_exists(total_steps, :ts), last_event_ts = :now"
            ),
            ConditionExpression="attribute_not_exists(approved_steps) OR NOT contains(approved_steps, :step)",
            # --- update_aggregate_on_approve 내 ExpressionAttributeValues (선택 강화) ---
            ExpressionAttributeValues={
                ":s": {int(step_index)},        # Number Set
                ":one": Decimal("1"),
                ":ts": Decimal(str(total_steps)),
                ":now": Decimal(str(time.time())),
                ":step": int(step_index)
            },
            ReturnValues="ALL_NEW"
        )
        return resp.get("Attributes", {})
    except progress_tbl.meta.client.exceptions.ConditionalCheckFailedException:
        resp = progress_tbl.get_item(Key={"mission_id": mission_id, "user_id_ts": f"agg#{user_id}"})
        return resp.get("Item", {}) or {}

# awarded_points 필드 제거, 대신 scoring_meta 정도만 유지 (참고용)
def ensure_single_completed_item(mission_id, user_id, approved_count, total_steps, scoring_meta=None):
    if approved_count is None or total_steps is None:
        return False
    if int(approved_count) < int(total_steps):
        return False
    details = {"approved_steps": int(approved_count), "total_steps": int(total_steps)}
    if scoring_meta:
        details["scoring_meta"] = _to_decimal(scoring_meta)
    try:
        progress_tbl.put_item(
            Item={
                "mission_id": mission_id,
                "user_id_ts": f"{user_id}#COMPLETED",
                "user_id": user_id,
                "step_index": -1,
                "status": "COMPLETED",
                "details": _to_decimal(details),
                "created_at": Decimal(str(time.time()))
            },
            ConditionExpression="attribute_not_exists(user_id_ts)"
        )
        return True
    except progress_tbl.meta.client.exceptions.ConditionalCheckFailedException:
        return False

# 기존 calc_points 함수 전체 삭제

# ---------- handler ----------
def lambda_handler(event, context):
    print("[EVENT]", list(event.keys()), "records=", len(event.get("Records", [])))
    print("[EXIF] PIL available:", Image is not None)

    if not event.get("Records"):
        dbg_bucket = os.getenv("DEBUG_BUCKET")
        dbg_key    = os.getenv("DEBUG_KEY")
        if dbg_bucket and dbg_key:
            print("[DEBUG] Fallback to DEBUG_BUCKET/KEY:", dbg_bucket, dbg_key)
            event = {"Records":[{"s3":{"bucket":{"name":dbg_bucket},"object":{"key":dbg_key}}}]}

    results = []
    touched_pairs = set()

    # 최신 프롬프트/모델 설정
    try:
        prompt_cfg = load_process_prompt()
        print("[PROMPT] cfg loaded:", {"model_id": prompt_cfg.get("model_id"),
                                       "threshold": prompt_cfg.get("confidence_threshold"),
                                       "key": prompt_cfg.get("_resolved_key")})
    except Exception as e:
        print("[PROMPT][ERROR] failed to load cfg:", repr(e))
        prompt_cfg = {"model_id":"anthropic.claude-3-haiku-20240307-v1:0","confidence_threshold":0.55,
                      "prompt_template": "사진 판정: {step_text} -> JSON 한 줄({match,confidence,reasons})"}

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key    = rec["s3"]["object"]["key"]

        try:
            # 1) S3에서 파일/메타데이터/서버시간
            data, content_type, meta, last_modified = get_object_bytes(bucket, key)
            media_type = guess_media_type(key, content_type)
            mission_id, user_id, step_index = parse_ids_from_meta_or_key(key, meta)

            def reject(reason, extra=None):
                det = {"reason": reason, "s3": {"bucket": bucket, "key": key}}
                if extra: det.update(extra)
                put_progress_log(mission_id or "UNKNOWN", user_id or "UNKNOWN", step_index, "REJECTED", det)
                print("[REJECT]", reason, det)
                results.append({"ok": False, "mission_id": mission_id, "user_id": user_id,
                                "step_index": step_index, "status": "REJECTED", "reason": reason})

            # (A) 시간 필터: startts <= uploaded_epoch <= deadlinets
            try:
                start_ts    = int(meta.get("startts"))
                deadline_ts = int(meta.get("deadlinets"))
            except Exception:
                reject("시간창 메타데이터 누락/파싱 실패"); continue

            uploaded_epoch = int(last_modified.timestamp()) if last_modified else int(time.time())
            if uploaded_epoch < start_ts:
                reject("모임 시작 이전 업로드", {"start_ts": start_ts, "uploaded_epoch": uploaded_epoch}); continue
            if uploaded_epoch > deadline_ts:
                reject("인증 허용 시간 초과", {"deadline_ts": deadline_ts, "uploaded_epoch": uploaded_epoch}); continue

            # (B) 위치 필터: 구 경계
            tmp_path = f"/tmp/{uuid.uuid4().hex}"
            with open(tmp_path, "wb") as f:
                f.write(data)
            gps = extract_gps_from_exif(tmp_path)
            if not is_within_district(gps):
                reject(f"위치가 {DISTRICT_NAME} 경계 밖(또는 GPS 없음)", {"gps": gps}); continue

            # (C) 비전 판정
            _, mission = fetch_mission_flat(mission_id)
            if not mission:
                reject("Missions_Live에 미션 없음"); continue
            steps = mission.get("steps") or []
            total_steps = len(steps)

            step_text = steps[step_index] if isinstance(step_index, int) and 0 <= step_index < total_steps else (mission.get("name") or "미션 단계 설명 없음")
            prompt = build_vision_prompt(step_text, prompt_cfg)
            img_b64 = base64.b64encode(data).decode("utf-8")
            verdict = ask_bedrock_vision(prompt_cfg["model_id"], prompt, img_b64, media_type)
            vision_ok = bool(verdict.get("match")) and float(verdict.get("confidence", 0.0)) >= float(prompt_cfg.get("confidence_threshold", 0.55))
            status = "APPROVED" if vision_ok else "REJECTED"

            details = {
                "s3": {"bucket": bucket, "key": key, "uploaded_epoch": uploaded_epoch},
                "media_type": media_type,
                "gps": gps,
                "vision": {"verdict": verdict, "ok": vision_ok, "threshold": prompt_cfg.get("confidence_threshold")},
                "step_text": step_text,
                "time_window": {"start_ts": start_ts, "deadline_ts": deadline_ts},
                "prompt_key": prompt_cfg.get("_resolved_key")
            }
            exif_ts = extract_exif_datetime_epoch(data)
            if exif_ts and (exif_ts + 12 * 3600) < start_ts:
                details["exif_warning"] = {"exif_captured_ts": exif_ts, "note": "EXIF가 매우 과거(참고용)"}

            put_progress_log(mission_id, user_id, step_index, status, details)

            # 집계/완료
            if status == "APPROVED":
                agg = update_aggregate_on_approve(mission_id, user_id, int(step_index), total_steps) or {}
                approved_count = int(agg.get("approved_count", 0) or 0)
                total_steps = int(total_steps or 0)    
                scoring_meta = {
                    "participants": int(mission.get("participants", 3)),
                    "difficulty": int(mission.get("difficulty", 1)),
                    # 필요하면 표시용으로만 남김. 계산은 외부 모듈이 수행.
                    # "base_per_person": 500  # <- 굳이 고정값을 남길 필요 없으면 생략
                }
                if ensure_single_completed_item(mission_id, user_id, approved_count, total_steps, scoring_meta):
                    print("[COMPLETE] created:", mission_id, user_id)
                else:
                    print("[COMPLETE] already-exists or not-yet:", mission_id, user_id)

            touched_pairs.add((mission_id, user_id))
            results.append({"ok": True, "mission_id": mission_id, "user_id": user_id, "step_index": step_index, "status": status})

        except Exception as e:
            print("[ERROR]", repr(e))
            results.append({"ok": False, "error": str(e), "bucket": bucket, "key": key})
            continue

    # 이벤트 배치 보정
    for mission_id, user_id in touched_pairs:
        resp = progress_tbl.get_item(Key={"mission_id": mission_id, "user_id_ts": f"agg#{user_id}"})
        agg = resp.get("Item", {}) or {}
        approved_count = int(agg.get("approved_count", 0) or 0)
        total_steps = int(agg.get("total_steps", 0) or 0)
        # scoring_meta는 표시용으로만 채움
        scoring_meta = None
        if ensure_single_completed_item(mission_id, user_id, approved_count, total_steps, scoring_meta):
            print("[RECONCILE] completed:", mission_id, user_id, approved_count, total_steps)

    print(json.dumps(results, ensure_ascii=False))
    return {"statusCode": 200, "body": json.dumps({"results": results}, ensure_ascii=False)}