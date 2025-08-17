import os
import json
import boto3
from decimal import Decimal
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')

DRAFT_TABLE_NAME = os.getenv('DRAFT_TABLE_NAME', 'MissionDrafts')
LIVE_TABLE_NAME  = os.getenv('LIVE_TABLE_NAME',  'Missions_Live')

draft_table = dynamodb.Table(DRAFT_TABLE_NAME)
live_table  = dynamodb.Table(LIVE_TABLE_NAME)

def _as_int(v, default=0):
    try:
        if v is None:
            return default
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
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

def lambda_handler(event, context):
    print("Received event:", json.dumps(event)[:2000])

    for rec in event.get('Records', []):
        if rec.get('eventName') not in ('MODIFY', 'INSERT'):
            continue

        new_img = rec.get('dynamodb', {}).get('NewImage', {}) or {}
        old_img = rec.get('dynamodb', {}).get('OldImage', {}) or {}

        new_status = (new_img.get('status', {}) or {}).get('S')
        old_status = (old_img.get('status', {}) or {}).get('S')

        # APPROVED로 '새로' 전이된 경우만 처리
        if new_status != 'APPROVED' or old_status == 'APPROVED':
            continue

        mission_id = _get_str(new_img, 'mission_id')
        if not mission_id:
            print("[SKIP] mission_id missing")
            continue

        # Draft에 저장한 생성 결과(JSON 문자열) 파싱
        mission_data = _get_json_str_field(new_img, 'mission_data') or {}

        try:
            # -------- 표준 필드 매핑 --------
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

            # -------- 썸네일/가이드: mission_data의 새 표준 키만 사용 --------
            thumb = mission_data.get('thumbnail_url') or ""
            guides = mission_data.get('guides_urls') or []
            if isinstance(guides, str):
                guides = [p.strip() for p in guides.split(",") if p.strip()]

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

                # ✅ 표준화: Live에는 sample_image_urls로 저장
                'thumbnail_url': thumb,
                'sample_image_urls': guides,

                'point_rule_text': point_rule,
                'created_at': now_iso,
                'updated_at': now_iso,
            }

            # 최초에는 생성을 시도하고, 이미 있으면 UPSERT로 갱신
            try:
                live_table.put_item(
                    Item=live_item,
                    ConditionExpression='attribute_not_exists(mission_id)'
                )
                print(f"[LIVE][CREATED] {mission_id}")
            except live_table.meta.client.exceptions.ConditionalCheckFailedException:
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
                        ':th': thumb, ':si': guides, ':pr': point_rule, ':u': now_iso
                    }
                )
                print(f"[LIVE][UPDATED] {mission_id}")

            # Draft는 PROCESSED로 마킹
            draft_table.update_item(
                Key={'mission_id': mission_id},
                UpdateExpression="SET #s = :processed",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':processed': 'PROCESSED'}
            )
            print(f"[DRAFT][MARKED PROCESSED] {mission_id}")

        except Exception as e:
            print(f"[ERROR] mission_id={mission_id} err={e}")
            # 오류 발생해도 다른 레코드 처리는 계속
            continue

    return {'statusCode': 200, 'body': json.dumps('ok')}