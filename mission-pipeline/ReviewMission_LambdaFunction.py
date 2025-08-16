import json
import boto3
from decimal import Decimal

# JSON 인코더가 DynamoDB의 Decimal 타입을 처리할 수 있도록 헬퍼 클래스를 정의합니다.
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # 정수이면 int로, 소수이면 float로 변환합니다.
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        return super(DecimalEncoder, self).default(obj)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('MissionDrafts')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    route_key = event.get('routeKey')
    path_parameters = event.get('pathParameters', {})

    try:
        # --- 1. 검수 대기중인 미션 목록 조회 ---
        if route_key == "GET /missions/pending":
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('status').eq('PENDING_REVIEW')
            )
            items = response.get('Items',)

            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps(items, cls=DecimalEncoder)
            }

        # --- 2. 특정 미션 '반려' 처리 ---
        elif route_key == "POST /missions/{mission_id}/reject":
            mission_id = path_parameters.get('mission_id')
            if not mission_id:
                return {'statusCode': 400, 'body': json.dumps({'error': 'mission_id is required'})}

            table.update_item(
                Key={'mission_id': mission_id},
                UpdateExpression="set #s = :s",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':s': 'REJECTED'}
            )

            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': f'Mission {mission_id} has been REJECTED.'})
            }

        # --- 3. 나머지 미션 '일괄 승인' 처리 ---
        elif route_key == "POST /missions/approve-all-pending":
            # 먼저 'PENDING_REVIEW' 상태인 모든 미션을 스캔합니다.
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('status').eq('PENDING_REVIEW')
            )
            pending_missions = response.get('Items',)

            # 각 미션의 상태를 'APPROVED'로 업데이트합니다.
            approved_count = 0
            for mission in pending_missions:
                table.update_item(
                    Key={'mission_id': mission['mission_id']},
                    UpdateExpression="set #s = :s",
                    ExpressionAttributeNames={'#s': 'status'},
                    ExpressionAttributeValues={':s': 'APPROVED'}
                )
                approved_count += 1

            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': f'Successfully approved {approved_count} pending missions.'})
            }

        else:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Not Found'})}

    except Exception as e:
        print(f"Error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}