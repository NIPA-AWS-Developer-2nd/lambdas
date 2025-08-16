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
        if v is None: return default
        return int(v)
    except Exception:
        try:
            # Decimal 등
            return int(Decimal(str(v)))
        except Exception:
            return default

def _ensure_list_str(x):
    # 문자열이면 쉼표 기준 분리 → 트림 → 빈값 제거
    if x is None:
        return []
    if isinstance(x, list):
        # list 안이 dict/기타면 문자열화
        return [str(s) for s in x]
    if isinstance(x, str):
        parts = [p.strip() for p in x.split(',')]
        return [p for p in parts if p]
    # 그 외 타입은 문자열화해서 단건 리스트
    return [str(x)]

def _get_str(new_image, key):
    node = new_image.get(key) or {}
    # Streams는 타입래퍼(S, N, L…)로 옴
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

def lambda_handler(event, context):
    print("Received event:", json.dumps(event)[:2000])

    for rec in event.get('Records', []):
        if rec.get('eventName') not in ('MODIFY', 'INSERT'):
            continue

        new_img = rec.get('dynamodb', {}).get('NewImage', {}) or {}
        old_img = rec.get('dynamodb', {}).get('OldImage', {}) or {}

        new_status = (new_img.get('status', {}) or {}).get('S')
        old_status = (old_img.get('status', {}) or {}).get('S')

        # 'APPROVED'로 새로 전이된 경우에만 처리
        if new_status != 'APPROVED' or old_status == 'APPROVED':
            continue

        mission_id = _get_str(new_img, 'mission_id')
        if not mission_id:
            print("[SKIP] mission_id missing")
            continue

        # Draft에 저장해둔 생성 결과(JSON 문자열) 파싱
        mission_data = _get_json_str_field(new_img, 'mission_data') or {}

        try:
            # ---- 필드 매핑(프론트/백엔 팀원이 쓰기 편한 형태) ----
            name_kr  = mission_data.get('Mission_Name_KR')
            category = mission_data.get('Interest_Category')
            tags     = _ensure_list_str(mission_data.get('Secondary_Tags'))  # 문자열배열 강제
            steps    = _ensure_list_str(mission_data.get('Verification_Steps'))

            # 선택/신규 필드들
            intro_kr          = mission_data.get('Intro_KR')                    # 소개문
            estimated_minutes = _as_int(mission_data.get('Estimated_Minutes'))  # 예상 소요
            cautions_kr       = _ensure_list_str(mission_data.get('Cautions_KR'))
            # 썸네일/가이드는 관리자 페이지에서 업로드 → S3 경로를 나중에 채움
            thumbnail_url     = mission_data.get('Thumbnail_URL') or ""
            sample_image_urls = _ensure_list_str(mission_data.get('Sample_Image_URLs'))

            # 난이도/인원
            difficulty   = _as_int(mission_data.get('Difficulty_Level'), 1)
            participants = _as_int(mission_data.get('Required_Participants'), 3)

            # 포인트 규칙(계산은 외부 모듈) — 텍스트로 남겨도 무방
            point_rule = mission_data.get('Point_Rule')  # 예: "기본 500 * 인원수 * 난이도"

            # 최종 Live 레코드
            live_item = {
                'mission_id': mission_id,
                'name': name_kr,
                'category': category,                 # "음식" | "문화/예술" | "스포츠" | "반려동물"
                'tags': tags,                         # text[]
                'difficulty': difficulty,             # int
                'participants': participants,         # int
                'steps': steps,                       # text[]
                'intro': intro_kr or "",              # text
                'estimated_minutes': estimated_minutes,   # int
                'cautions': cautions_kr,              # text[]
                'thumbnail_url': thumbnail_url,       # text
                'sample_image_urls': sample_image_urls,   # text[]
                'point_rule_text': point_rule or "",  # 계산은 외부 모듈, 텍스트로만 보존
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }

            # 이미 존재하면 덮어쓰지 않도록(중복 방지) — 필요 시 UPSERT로 바꿀 수 있음
            live_table.put_item(
                Item=live_item,
                ConditionExpression='attribute_not_exists(mission_id)'
            )
            print(f"[LIVE][OK] {mission_id}")

            # Draft 상태 갱신(선택) — 이미 APPROVED인 걸 PROCESSED로 바꾸고 싶을 때
            draft_table.update_item(
                Key={'mission_id': mission_id},
                UpdateExpression="SET #s = :processed",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':processed': 'PROCESSED'}
            )
            print(f"[DRAFT][MARKED PROCESSED] {mission_id}")

        except draft_table.meta.client.exceptions.ConditionalCheckFailedException:
            # 이미 Live에 있음 → 스킵
            print(f"[LIVE][SKIP] already exists: {mission_id}")
            continue
        except Exception as e:
            print(f"[ERROR] mission_id={mission_id} err={e}")
            # 실패는 레코드만 스킵하고 다음으로 진행
            continue

    return {'statusCode': 200, 'body': json.dumps('ok')}