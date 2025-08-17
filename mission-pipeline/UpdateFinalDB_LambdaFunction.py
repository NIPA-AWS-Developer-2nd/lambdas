import os
import json
import boto3
from decimal import Decimal
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

DRAFT_TABLE_NAME = os.getenv('DRAFT_TABLE_NAME', 'MissionDrafts')
LIVE_TABLE_NAME  = os.getenv('LIVE_TABLE_NAME',  'Missions_Live')

# 이미지 버킷/프리픽스 (관리자 페이지 업로드 규칙과 일치)
PROMPTS_BUCKET   = os.getenv('PROMPTS_BUCKET', 'halsaram-prompts')
THUMBNAIL_PREFIX = os.getenv('THUMBNAIL_PREFIX', 'thumbnail/')
GUIDES_PREFIX    = os.getenv('GUIDES_PREFIX', 'guides/')

draft_table = dynamodb.Table(DRAFT_TABLE_NAME)
live_table  = dynamodb.Table(LIVE_TABLE_NAME)

def _as_int(v, default=0):
    try:
        if v is None: return default
        return int(v)
    except Exception:
        try:
            return int(Decimal(str(v)))
        except Exception:
            return default

def _ensure_list_str(x):
    if x is None:
        return []
    if isinstance(x, list):
        return [str(s) for s in x]
    if isinstance(x, str):
        parts = [p.strip() for p in x.split(',')]
        return [p for p in parts if p]
    return [str(x)]

def _get_str(new_image, key):
    node = new_image.get(key) or {}
    if 'S' in node: return node['S']
    if 'N' in node: return node['N']
    return None

def _get_json_str_field(new_image, key):
    s = _get_str(new_image, key)
    if not s: return None
    try:
        return json.loads(s)
    except Exception:
        return None

def _looks_like_url(v: str) -> bool:
    return isinstance(v, str) and (v.startswith('http://') or v.startswith('https://'))

def _to_https_url(bucket: str, key: str) -> str:
    # S3 퍼블릭 버킷으로 열어둔 상태라고 하셨으니 단순 URL 구성
    return f"https://{bucket}.s3.amazonaws.com/{key}"

def _head_ok(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def lambda_handler(event, context):
    print("Received event:", json.dumps(event)[:2000])

    for rec in event.get('Records', []):
        if rec.get('eventName') not in ('MODIFY', 'INSERT'):
            continue

        new_img = rec.get('dynamodb', {}).get('NewImage', {}) or {}
        old_img = rec.get('dynamodb', {}).get('OldImage', {}) or {}

        new_status = (new_img.get('status', {}) or {}).get('S')
        old_status = (old_img.get('status', {}) or {}).get('S')

        # 'APPROVED'로 새로 전이된 경우만 처리
        if new_status != 'APPROVED' or old_status == 'APPROVED':
            continue

        mission_id = _get_str(new_img, 'mission_id')
        if not mission_id:
            print("[SKIP] mission_id missing")
            continue

        # Draft에 저장해둔 생성 결과(JSON 문자열)
        mission_data = _get_json_str_field(new_img, 'mission_data') or {}

        # 1) 기본 필드 매핑
        name_kr  = mission_data.get('Mission_Name_KR')
        category = mission_data.get('Interest_Category')
        tags     = _ensure_list_str(mission_data.get('Secondary_Tags'))
        steps    = _ensure_list_str(mission_data.get('Verification_Steps'))

        intro_kr          = mission_data.get('Intro_KR') or ""
        estimated_minutes = _as_int(mission_data.get('Estimated_Minutes'))
        cautions_kr       = _ensure_list_str(mission_data.get('Cautions_KR'))

        difficulty   = _as_int(mission_data.get('Difficulty_Level'), 1)
        participants = _as_int(mission_data.get('Required_Participants'), 3)

        point_rule = mission_data.get('Point_Rule') or ""

        # 2) 썸네일/가이드 URL 결정 로직
        # 2-1) Streams NewImage에 백엔드가 키를 직접 넣어줬는지
        thumbnail_key = _get_str(new_img, 'thumbnail_key')  # 예: "thumbnail/{mission_id}_1.jpg"
        guide_keys    = _ensure_list_str(_get_str(new_img, 'guide_keys'))  # 콤마 문자열일 수도 있어 처리

        # 2-2) mission_data에 URL로만 들어있는 경우(기존 호환)
        thumbnail_url_md = mission_data.get('Thumbnail_URL') or ""
        sample_urls_md   = _ensure_list_str(mission_data.get('Sample_Image_URLs'))

        # 2-3) 최종 URL 조합
        if thumbnail_key:
            if not _looks_like_url(thumbnail_key):
                # 키라면 URL로 변환
                if not thumbnail_key.startswith(THUMBNAIL_PREFIX):
                    # 안전장치: 의도와 다르면 접두어 보정
                    thumbnail_key = THUMBNAIL_PREFIX + thumbnail_key
                # 존재 보강(선택)
                if _head_ok(PROMPTS_BUCKET, thumbnail_key):
                    thumbnail_url = _to_https_url(PROMPTS_BUCKET, thumbnail_key)
                else:
                    print(f"[WARN] thumbnail object not found: s3://{PROMPTS_BUCKET}/{thumbnail_key}")
                    thumbnail_url = _to_https_url(PROMPTS_BUCKET, thumbnail_key)  # 그래도 기록
            else:
                thumbnail_url = thumbnail_key
        else:
            # 키가 없다면 mission_data의 URL(혹은 빈값)
            thumbnail_url = thumbnail_url_md

        final_sample_urls = []
        if guide_keys:
            for k in guide_keys:
                k = k.strip()
                if not k:
                    continue
                if _looks_like_url(k):
                    final_sample_urls.append(k)
                else:
                    # 키라면 접두어 보정
                    if not (k.startswith(GUIDES_PREFIX) or k.startswith(THUMBNAIL_PREFIX)):
                        k = GUIDES_PREFIX + k
                    if _head_ok(PROMPTS_BUCKET, k):
                        final_sample_urls.append(_to_https_url(PROMPTS_BUCKET, k))
                    else:
                        print(f"[WARN] guide object not found: s3://{PROMPTS_BUCKET}/{k}")
                        final_sample_urls.append(_to_https_url(PROMPTS_BUCKET, k))
        else:
            # 키가 없다면 mission_data의 URL 리스트 사용(혹은 빈 리스트)
            final_sample_urls = sample_urls_md

        # 3) Live 아이템 구성
        now_iso = datetime.now(timezone.utc).isoformat()
        live_item = {
            'mission_id': mission_id,
            'name': name_kr,
            'category': category,
            'tags': tags,
            'difficulty': difficulty,
            'participants': participants,
            'steps': steps,
            'intro': intro_kr,
            'estimated_minutes': estimated_minutes,
            'cautions': cautions_kr,
            'thumbnail_url': thumbnail_url,
            'sample_image_urls': final_sample_urls,
            'point_rule_text': point_rule,
            'created_at': now_iso,
            'updated_at': now_iso,
        }

        try:
            live_table.put_item(
                Item=live_item,
                ConditionExpression='attribute_not_exists(mission_id)'
            )
            print(f"[LIVE][OK] {mission_id}")
        except live_table.meta.client.exceptions.ConditionalCheckFailedException:
            # 이미 있으면 업데이트(썸네일/가이드만 갱신되는 경우 대비)
            live_table.update_item(
                Key={'mission_id': mission_id},
                UpdateExpression=(
                    "SET #n=:n, category=:c, tags=:t, difficulty=:d, participants=:p, "
                    "steps=:s, intro=:i, estimated_minutes=:em, cautions=:ca, "
                    "thumbnail_url=:th, sample_image_urls=:si, point_rule_text=:pr, "
                    "updated_at=:u"
                ),
                ExpressionAttributeNames={'#n': 'name'},
                ExpressionAttributeValues={
                    ':n': name_kr, ':c': category, ':t': tags, ':d': difficulty, ':p': participants,
                    ':s': steps, ':i': intro_kr, ':em': estimated_minutes, ':ca': cautions_kr,
                    ':th': thumbnail_url, ':si': final_sample_urls, ':pr': point_rule, ':u': now_iso
                }
            )
            print(f"[LIVE][UPSERT] {mission_id}")

        # Draft는 PROCESSED로 마킹(중복 방지)
        draft_table.update_item(
            Key={'mission_id': mission_id},
            UpdateExpression="SET #s = :processed",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':processed': 'PROCESSED'}
        )
        print(f"[DRAFT][MARKED PROCESSED] {mission_id}")

    return {'statusCode': 200, 'body': json.dumps('ok')}