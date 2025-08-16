import json
import boto3
import uuid
from decimal import Decimal
import time
import urllib.request
import base64
import os

# ---- Boto3 clients
bedrock_runtime = boto3.client('bedrock-runtime', region_name=os.getenv('AWS_REGION', 'ap-northeast-2'))
dynamodb = boto3.resource('dynamodb')
secrets_manager = boto3.client('secretsmanager')
s3 = boto3.client('s3')

# ---- ENV
TABLE_NAME = os.getenv('TABLE_NAME', 'MissionDrafts')
SECRET_NAME = os.getenv('SLACK_SECRET_NAME', 'MissionNotifier/SlackWebhook')

PROMPTS_BUCKET = os.getenv('PROMPTS_BUCKET', 'halsaram-prompts')
GENERATE_PROMPTS_PREFIX = os.getenv('GENERATE_PROMPTS_PREFIX', 'generatePrompts/')

MODEL_ID_DEFAULT = 'anthropic.claude-3-haiku-20240307-v1:0'  # fallback

# ---- S3 helpers (❗누락되면 NameError)
def _get_latest_key(bucket: str, prefix: str) -> str:
    """prefix 내에서 마지막 수정시간이 가장 최신인 객체 Key를 반환 (폴더 객체 제외)."""
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
            if (latest is None) or (obj['LastModified'] > latest['LastModified']):
                latest = obj
        if not resp.get('IsTruncated'):
            break
        continuation = resp.get('NextContinuationToken')
    if not latest:
        raise FileNotFoundError(f'No prompt file found under s3://{bucket}/{prefix}')
    return latest['Key']

def _load_text_json_from_s3(bucket: str, key: str) -> dict:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))

def get_prompt_from_s3_latest() -> dict:
    key = _get_latest_key(PROMPTS_BUCKET, GENERATE_PROMPTS_PREFIX)
    cfg = _load_text_json_from_s3(PROMPTS_BUCKET, key)
    cfg['_resolved_key'] = key  # 디버깅용
    return cfg

# ---- Slack secret
def get_slack_webhook_url():
    res = secrets_manager.get_secret_value(SecretId=SECRET_NAME)
    if 'SecretString' in res:
        secret_str = res['SecretString']
    else:
        secret_str = base64.b64encode(res['SecretBinary']).decode('utf-8')
    try:
        parsed = json.loads(secret_str)
        if isinstance(parsed, dict) and 'webhook_url' in parsed:
            return parsed['webhook_url']
        if isinstance(parsed, dict):
            for v in parsed.values():
                if isinstance(v, str) and v.startswith('http'):
                    return v
        if isinstance(parsed, str) and parsed.startswith('http'):
            return parsed
    except Exception:
        if isinstance(secret_str, str) and secret_str.startswith('http'):
            return secret_str
    raise RuntimeError('Slack webhook URL을 Secret에서 찾을 수 없습니다.')

# ---- Prompt helpers
def _as_text_content(s: str):
    return [{"type": "text", "text": s}]

def build_few_shot_messages(prompt_config):
    """
    few_shot_examples 지원 형태:
    - [{"user":"...","assistant":"..."}]
    - [{"input":"...","output":"..."}]
    - [{"Mission_Name_KR":...}]  -> assistant-only 예시로 변환
    """
    messages = []
    examples = prompt_config.get('few_shot_examples', [])
    for ex in examples:
        if isinstance(ex, dict) and 'user' in ex and 'assistant' in ex:
            u = ex['user'] if isinstance(ex['user'], str) else json.dumps(ex['user'], ensure_ascii=False)
            a = ex['assistant'] if isinstance(ex['assistant'], str) else json.dumps(ex['assistant'], ensure_ascii=False)
            messages.append({"role": "user", "content": _as_text_content(u)})
            messages.append({"role": "assistant", "content": _as_text_content(a)})
            continue
        if isinstance(ex, dict) and 'input' in ex and 'output' in ex:
            u = ex['input'] if isinstance(ex['input'], str) else json.dumps(ex['input'], ensure_ascii=False)
            a = ex['output'] if isinstance(ex['output'], str) else json.dumps(ex['output'], ensure_ascii=False)
            messages.append({"role": "user", "content": _as_text_content(u)})
            messages.append({"role": "assistant", "content": _as_text_content(a)})
            continue
        if isinstance(ex, dict):
            messages.append({"role": "user", "content": _as_text_content("조건에 맞는 미션 1개를 JSON으로만 출력하세요.")})
            messages.append({"role": "assistant", "content": _as_text_content(json.dumps(ex, ensure_ascii=False))})
            continue
    return messages

def extract_json_array(text):
    try:
        return json.loads(text)
    except Exception:
        start = text.find('[')
        end = text.rfind(']')
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end+1])
        raise

# ---- Handler
def lambda_handler(event, context):
    # 1) 프롬프트 로드(최신 선택)
    prompt_config = get_prompt_from_s3_latest()
    print("[PROMPT] using:", prompt_config.get('_resolved_key'))
    system_prompt = prompt_config.get('system_prompt', '')
    user_prompt_template = prompt_config['user_prompt_template']
    model_id = prompt_config.get('model_id', MODEL_ID_DEFAULT)

    # 2) few-shot 메시지 구성
    messages = build_few_shot_messages(prompt_config)

    # 3) 실제 요청 메시지 추가
    num_missions_to_generate = int((event or {}).get("generate_count") or 5)
    few_shot_str = json.dumps(prompt_config.get('few_shot_examples', []), ensure_ascii=False, indent=2)
    final_user_prompt = user_prompt_template.format(
        num_missions=num_missions_to_generate,
        few_shot_examples=few_shot_str
    )
    messages.append({"role": "user", "content": _as_text_content(final_user_prompt)})

    # 4) Bedrock 호출
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "system": system_prompt,
        "messages": messages
    })
    response = bedrock_runtime.invoke_model(body=body, modelId=model_id)
    response_body = json.loads(response['body'].read())

    # 5) 응답 파싱
    texts = []
    for block in response_body.get('content', []):
        if block.get('type') == 'text' and 'text' in block:
            texts.append(block['text'])
    mission_drafts_str = "\n".join(texts).strip()
    model_missions = []
    try:
        model_missions = extract_json_array(mission_drafts_str)
        if not isinstance(model_missions, list):
            model_missions = []
    except Exception as e:
        print("[PARSE][WARN] model output parse failed:", repr(e))

    # 6) 모델 생성 결과 + extra_missions 합치고 한 번에 저장
    table = dynamodb.Table(TABLE_NAME)
    created = 0

    extra_missions = (event or {}).get("extra_missions") or []
    print("[SAVE] model_missions:", len(model_missions), "extra_missions:", len(extra_missions))

    # 배치 내 중복 mission_id 방지 집합
    seen_ids = set()

    def _normalize_and_validate(m: dict) -> dict | None:
        # 필수 필드
        name = m.get("Mission_Name_KR")
        steps = m.get("Verification_Steps")
        if not name or not isinstance(steps, list) or not steps:
            print("[SKIP] required fields missing:", {"Mission_Name_KR": name, "steps_type": type(steps)})
            return None

        # Secondary_Tags: 문자열 -> 배열(콤마 분리)
        tags = m.get("Secondary_Tags")
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]
            m["Secondary_Tags"] = tags
        elif not isinstance(tags, list):
            m["Secondary_Tags"] = []

        # 정수 필드 정규화
        for k in ("Difficulty_Level", "Required_Participants", "Estimated_Minutes"):
            if k in m and isinstance(m[k], str) and m[k].isdigit():
                m[k] = int(m[k])

        # Scoring 기본값 보강
        m.setdefault("Scoring", {})
        sc = m["Scoring"]
        sc.setdefault("Base_Per_Person", 500)
        sc.setdefault("Participants", m.get("Required_Participants", 3))
        sc.setdefault("Difficulty_Multiplier", m.get("Difficulty_Level", 1))
        sc.setdefault("Host_Bonus", 200)
        sc.setdefault("Duplicate_Penalty_Factor", 0.5)

        # Point_Rule 문자열 생성
        try:
            base = int(sc["Base_Per_Person"])
            ppl  = int(sc["Participants"])
            diff = int(sc["Difficulty_Multiplier"])
            m["Point_Rule"] = f"기본 {base} * 인원수({ppl}) * 난이도({diff}) = {base*ppl*diff} 포인트"
        except Exception:
            pass

        return m

    all_missions = (model_missions if isinstance(model_missions, list) else []) + extra_missions
    print("[SAVE] total to insert:", len(all_missions))

    for m in all_missions:
        nm = _normalize_and_validate(m)
        if not nm:
            continue

        # 외부에서 mission_id 지정 가능 (예: test-norunsan-001)
        mission_id = nm.get("mission_id") or str(uuid.uuid4())
        if mission_id in seen_ids:
            print("[SKIP] duplicated in batch:", mission_id)
            continue
        seen_ids.add(mission_id)

        item = {
            'mission_id': mission_id,
            'status': 'PENDING_REVIEW',
            'mission_data': json.dumps(nm, ensure_ascii=False),
            'created_at': Decimal(str(time.time()))
        }
        try:
            table.put_item(Item=item)
            created += 1
            print("[OK] inserted:", mission_id, nm.get("Mission_Name_KR"))
        except Exception as e:
            print("[ERR] put_item failed:", repr(e), mission_id)

    # 7) Slack 알림
    if created > 0:
        try:
            webhook_url = get_slack_webhook_url()
            slack_message = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn",
                                 "text": f"🔔 *새 미션 {created}개가 검수를 기다립니다!*"}
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn",
                                 "text": "👉 관리자 페이지에서 승인/반려를 진행해주세요.\n"
                                         "• URL: https://admin.halsaram.site/\n"
                                         "• 접속 시, 발급된 *인증키*를 입력해 주세요."}
                    }
                ]
            }
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(slack_message).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            urllib.request.urlopen(req)
        except Exception as e:
            print("[SLACK][WARN]", repr(e))

    return {
        'statusCode': 200,
        'body': json.dumps({
            'created': created,
            'prompt_key': prompt_config.get('_resolved_key'),
            'generated_count': len(model_missions),
            'extra_count': len(extra_missions)
        }, ensure_ascii=False)
    }