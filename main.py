# -*- coding: utf-8 -*-
import requests
import time
import mysql.connector
from mysql.connector import Error

from insert_sql import find_or_insert_housing_complex, find_or_insert_housing_type, insert_real_estate
from naver_api import fetch_location_data, fetch_property_data, fetch_article_filter_data

header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.220 Whale/1.3.51.7 Safari/537.36',
    'Referer': 'https://m.land.naver.com/'
}

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='localhost',       # MySQL 호스트
            user='root',   # MySQL 사용자 이름
            password='root1234',  # MySQL 비밀번호
            database='omnm'  # 연결할 데이터베이스
        )
        if connection.is_connected():
            print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def get_value(data, keys, default=None):
    for key in keys:
        data = data.get(key, default)
        if data is default:
            break
    return data

# MySQL에 데이터 삽입
connection = connect_to_mysql()
if connection:
    city = ['강남구', '강동구', '강북구', '강서구', '관악구', '광진구', '구로구', '금천구', '노원구', '도봉구', '동대문구', '동작구', '마포구', '서대문구', '서초구',
            '성동구',
            '성북구', '송파구', '양천구', '영등포구', '용산구', '은평구', '종로구', '중구', '중랑구']

    for gu in city:
        gu_location_data = fetch_location_data(gu, header)
        if not gu_location_data:
            continue

        page = 0

        while True:
            page += 1
            gu_results = fetch_property_data(gu_location_data, page, header)
            if not gu_results:
                break  # 매물이 없으면 루프 종료

            # 매물 데이터 요청과 저장 루프 안에서
            for item in gu_results:
                property_data = {
                    "gu": gu,
                    "atclNo": item.get('atclNo'),
                    "atclNm": item.get('atclNm'),  # 매물 이름
                    "rletTpCd": item.get('rletTpCd'),  # 매물 타입
                    "tradTpNm": item.get('tradTpNm'),  # 거래 타입
                    "bildNm": item.get('bildNm'),  # 건물 이름
                    "flrInfo": item.get('flrInfo'),  # 층 정보
                    "prc": item.get('prc'),  # 가격 정보
                    "cpNm": item.get("cpNm"),  # 중개업소명
                    "cortarNo": item.get("cortarNo"),  # 구 코드
                    "lat": item.get("lat"),  # 위도 정보
                    "lng": item.get("lng"),  # 경도 정보
                    "img": item.get("repImgUrl")
                }
                article_url = "https://fin.land.naver.com/articles/" + item.get('atclNo')
                article_filter_data = fetch_article_filter_data(article_url)

                estate_overall_data = {
                    'basicInfo' : property_data,
                    'estateKeyInfo' : article_filter_data[0]['state']['data']['result'],
                    'priceInfo' : article_filter_data[2]['state']['data']['result'],
                    'addressInfo' : article_filter_data[4]['state']['data']['result'],
                    'maintenanceInfo' : article_filter_data[5]['state']['data']['result'],
                    'floorPlanInfo' : article_filter_data[6]['state']['data']['result'],
                    'brokerInfo' : article_filter_data[8]['state']['data']['result']
                }

                pyeongListUrl = "https://fin.land.naver.com/front-api/v1/complex/pyeong"
                pyeongListparams = {
                    "complexNumber": estate_overall_data['estateKeyInfo']['key']['complexNumber'],
                    "pyeongTypeNumber": estate_overall_data['estateKeyInfo']['key']['pyeongTypeNumber']
                }
                pyeong_list_response = requests.get(pyeongListUrl, params=pyeongListparams)
                pyeong_list_response = pyeong_list_response.json()

                try:
                    cursor = connection.cursor(prepared=True)
                    # housing_complex 확인 및 삽입
                    housing_complex_id = find_or_insert_housing_complex(cursor, estate_overall_data)

                    # housing_type 확인 및 삽입
                    housing_type_id = find_or_insert_housing_type(cursor, housing_complex_id, pyeong_list_response,
                                                                  estate_overall_data)

                    # real_estate 삽입
                    insert_real_estate(cursor, housing_type_id, estate_overall_data)

                    connection.commit()
                    print(f"{cursor.lastrowid} 매물 데이터 생성 완료")
                except Error as e:
                    print(f"Error: {e}")
                finally:
                    cursor.close()

                time.sleep(0.5)  # 서버 과부하 방지를 위한 딜레이

    connection.close()