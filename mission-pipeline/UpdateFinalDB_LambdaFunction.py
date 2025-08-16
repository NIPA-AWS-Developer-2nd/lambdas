import json
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
# 최종 운영 DB 테이블 이름을 'Missions_Live'로 지정
live_table = dynamodb.Table('Missions_Live')
draft_table = dynamodb.Table('MissionDrafts')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    for record in event.get('Records',):
        # DynamoDB Stream에서 변경된 항목의 정보를 가져옵니다.
        if record.get('eventName') == 'MODIFY':
            new_image = record.get('dynamodb', {}).get('NewImage', {})

            # 상태(status)가 'APPROVED'로 변경되었는지 확인합니다.
            status = new_image.get('status', {}).get('S')
            if status == 'APPROVED':
                mission_id = new_image.get('mission_id', {}).get('S')
                mission_data_str = new_image.get('mission_data', {}).get('S')

                if mission_id and mission_data_str:
                    try:
                        # mission_data는 JSON 문자열이므로 파싱합니다.
                        mission_data = json.loads(mission_data_str)

                        # 최종 운영 DB에 저장할 데이터를 구성합니다. (백엔드가 사용하기 편한 필드명으로 정리)
                        item_to_live = {
                            'mission_id': mission_id,
                            'name': mission_data.get('Mission_Name_KR'),
                            'category': mission_data.get('Interest_Category'),
                            'tags': mission_data.get('Secondary_Tags'),
                            'difficulty': Decimal(str(mission_data.get('Difficulty_Level', 0))),
                            'participants': Decimal(str(mission_data.get('Required_Participants', 0))),
                            'steps': mission_data.get('Verification_Steps')
                        }

                        # 최종 운영 DB (Missions_Live)에 저장합니다.
                        live_table.put_item(Item=item_to_live)
                        print(f"Successfully moved mission {mission_id} to Missions_Live table.")

                        # 원본 초안의 상태를 'PROCESSED'로 변경하여 중복 처리를 방지합니다.
                        draft_table.update_item(
                            Key={'mission_id': mission_id},
                            UpdateExpression="set #s = :s",
                            ExpressionAttributeNames={'#s': 'status'},
                            ExpressionAttributeValues={':s': 'PROCESSED'}
                        )
                        print(f"Updated status of mission {mission_id} in MissionDrafts to PROCESSED.")

                    except Exception as e:
                        print(f"Error processing mission {mission_id}: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete.')
    }