# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import json
import time
import math
import mysql.connector
from mysql.connector import Error

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

def calculate_bounds(lat, lon, zoom):
    R = 6378137  # 지구 반지름 (미터)
    scale = 2 ** zoom

    # Mercator Projection
    x = R * math.radians(lon)
    y = R * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))

    # 지도 영역 크기 (단위: 미터)
    half_width = 5000 / scale  # 가로 반경
    half_height = 5000 / scale  # 세로 반경

    # 경계 계산
    lft = math.degrees((x - half_width) / R)
    rgt = math.degrees((x + half_width) / R)
    btm = math.degrees(2 * math.atan(math.exp((y - half_height) / R)) - math.pi / 2)
    top = math.degrees(2 * math.atan(math.exp((y + half_height) / R)) - math.pi / 2)

    return {'btm': btm, 'top': top, 'lft': lft, 'rgt': rgt}

def get_value(data, keys, default=None):
    for key in keys:
        data = data.get(key, default)
        if data is default:
            break
    return data

# MySQL에 데이터 삽입
connection = connect_to_mysql()
if connection:
    # 구 목록 및 헤더 설정
    city = ['금천구']

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.220 Whale/1.3.51.7 Safari/537.36',
        'Referer': 'https://m.land.naver.com/'
    }

    # 매물 데이터를 요청하는 URL
    property_url = "https://m.land.naver.com/cluster/ajax/articleList"

    # gu.txt 파일을 열어서 데이터를 저장 (w 모드)
    with open("gu.txt", 'w', encoding='utf-8') as f:
        for gu in city:
            # 구별로 위치 정보 URL 요청
            gu_url = "https://m.land.naver.com/search/result/" + gu
            gu_response = requests.get(gu_url, headers=header)

            if gu_response.status_code == 200:
                gu_html = gu_response.text
                gu_soup = BeautifulSoup(gu_html, 'html.parser')

                # 위치 정보를 필터 데이터에서 추출
                gu_filter_data = re.findall('filter: {(.+?)},', str(gu_soup.select('script')[4]), flags=re.DOTALL)
                gu_location_data = {}
                try:
                    # 필터 데이터를 위치 정보로 변환
                    gu_filter_parts = gu_filter_data[0].split()
                    for j in range(len(gu_filter_parts)):
                        if j % 2 == 0:
                            gu_location_data[gu_filter_parts[j].strip(":")] = gu_filter_parts[j + 1].strip(',').strip("'")
                except IndexError:
                    pass

                # 매물 데이터 요청에 필요한 위치 정보 추출
                gu_lat = gu_location_data.get('lat', '')
                gu_lon = gu_location_data.get('lon', '')
                gu_cortarNo = gu_location_data.get('cortarNo', '')
                gu_btm = calculate_bounds(float(gu_lat), float(gu_lon), 12).get('btm')
                gu_top = calculate_bounds(float(gu_lat), float(gu_lon), 12).get('top')
                gu_lft = calculate_bounds(float(gu_lat), float(gu_lon), 12).get('lft')
                gu_rgt = calculate_bounds(float(gu_lat), float(gu_lon), 12).get('rgt')

                # 매물 데이터 요청
                gu_param = {
                    'lat': gu_lat,
                    'lon': gu_lon,
                    'btm': gu_btm,
                    'top': gu_top,
                    'lft': gu_lft,
                    'rgt': gu_rgt,
                    'cortarNo': gu_cortarNo,
                    'rletTpCd': 'APT',  # 매물 타입 (APT: 아파트)
                    'tradTpCd': 'B1:B2',  # 거래 타입 (B1:전세, B2:월세)
                    'z': '12',  # 지도 줌 레벨
                    'sort': 'rank'
                }

                page = 0
                while True:
                    page += 1
                    gu_param['page'] = page

                    gu_resp = requests.get(property_url, params=gu_param, headers=header)
                    if gu_resp.status_code != 200:
                        print(f'Error fetching data for {gu}, page {page}')
                        break

                    gu_data = gu_resp.json()
                    gu_results = gu_data.get('body', [])
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
                        article_num = item.get('atclNo')
                        article_url = "https://fin.land.naver.com/articles/" + article_num
                        article_response = requests.get(article_url)

                        if article_response.status_code == 200:
                            article_html = article_response.text
                            article_soup = BeautifulSoup(article_html, 'html.parser')

                            # 위치 정보를 필터 데이터에서 추출
                            article_filter_data= json.loads(str(article_soup.select('script')[40].string))['props']['pageProps']['dehydratedState']['queries']

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

                                #hosing_complex 테이블의 확인
                                housing_complex_id = -1
                                find_housing_complex = "SELECT housing_complex_id FROM housing_complex WHERE code = %s"
                                cursor.execute(find_housing_complex, (int(estate_overall_data['estateKeyInfo']['key']['complexNumber']),))
                                result = cursor.fetchone()

                                if result is None:
                                    # area_code_id 찾기
                                    find_area_code_id = """
                                        SELECT area_code_id FROM area_code WHERE code = %s
                                    """
                                    cursor.execute(find_area_code_id, (int(estate_overall_data['basicInfo']['cortarNo']),))
                                    area_code_id = cursor.fetchone()

                                    # hosing_complex 테이블 삽입
                                    complex_insert_sql = """
                                        INSERT INTO housing_complex(
                                            area_code_id, code, name, address, unit_count, established_date,
                                            system_type, energy_type, parking_count, dong_count, floor_area_ratio, 
                                            building_coverage_ratio, construction_company
                                        )
                                        VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                    """

                                    complex_insert_data = (
                                    area_code_id[0],
                                    estate_overall_data['estateKeyInfo']['key']['complexNumber'],
                                    estate_overall_data['basicInfo']['atclNm'],
                                    estate_overall_data['addressInfo']['address']['legalDivision'] + ' ' +
                                    estate_overall_data['addressInfo']['address']['roadName'],
                                    estate_overall_data['addressInfo']['totalHouseholdNumber'],
                                    estate_overall_data['addressInfo']['useApprovalDate'],
                                    estate_overall_data['addressInfo']['heatingAndCoolingInfo'][
                                        'heatingAndCoolingSystemType'],
                                    estate_overall_data['addressInfo']['heatingAndCoolingInfo']['heatingEnergyType'],
                                    estate_overall_data['addressInfo']['parkingInfo']['totalParkingCount'],
                                    estate_overall_data['addressInfo']['dongCount'],
                                    estate_overall_data['addressInfo']['buildingRatioInfo']['floorAreaRatio'],
                                    estate_overall_data['addressInfo']['buildingRatioInfo']['buildingCoverageRatio'],
                                    estate_overall_data['addressInfo']['constructionCompany'],
                                    )

                                    cursor.execute(complex_insert_sql, complex_insert_data)
                                    connection.commit()
                                    housing_complex_id = cursor.lastrowid
                                else:
                                    housing_complex_id = result[0]
                                    
                                #housing_type 테이블의 확인
                                housing_type_id = -1
                                find_housing_type = "SELECT housing_type_id FROM housing_type WHERE housing_complex_id = %s AND code = %s"

                                find_housing_type_data = (
                                    housing_complex_id,
                                    pyeong_list_response['result']['number']
                                )
                                cursor.execute(find_housing_type, find_housing_type_data)
                                find_housing_type_data_result = cursor.fetchone()

                                if find_housing_type_data_result is None:
                                    # housing_type 테이블 삽입
                                    type_insert_sql = """
                                        INSERT INTO housing_type(housing_complex_id, code, name, unit_count, entrance_type, supply_area_size, exclusive_area_size, management_fee, room_count, 
                                        bathroom_count, floor_plan_img_url, floor_plan_link)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """

                                    floor_plan_urls = ''
                                    if len(pyeong_list_response['result']['floorPlanUrls']) > 0 :
                                        if "BASE" in pyeong_list_response['result']['floorPlanUrls']:
                                            floor_plan_urls = pyeong_list_response['result']['floorPlanUrls']['BASE']['0'][2]
                                        else:
                                            floor_plan_urls = pyeong_list_response['result']['floorPlanUrls']['EXPN']['OPT1'][2]

                                    
                                    type_insert_data = (
                                        housing_complex_id,
                                        pyeong_list_response['result']['number'],
                                        pyeong_list_response['result']['name'],
                                        pyeong_list_response['result']['unitsOfSameArea'],
                                        pyeong_list_response['result']['entranceType'],
                                        pyeong_list_response['result']['supplyArea'],
                                        pyeong_list_response['result']['exclusiveArea'],
                                        estate_overall_data['maintenanceInfo']['yearMonthFee'],
                                        pyeong_list_response['result']['roomCount'],
                                        pyeong_list_response['result']['bathRoomCount'],
                                        floor_plan_urls,
                                        "https://fin.land.naver.com/complexes/" + str(estate_overall_data['estateKeyInfo']['key']['complexNumber'])
                                        + "?tab=complex-info",
                                    )

                                    cursor.execute(type_insert_sql, type_insert_data)
                                    connection.commit()
                                    housing_type_id = cursor.lastrowid
                                else:
                                    housing_type_id = find_housing_type_data_result[0]
                                
                                # real_estate 삽입
                                real_estate_latitude = estate_overall_data['basicInfo']['lat']
                                real_estate_longitude = estate_overall_data['basicInfo']['lng']
                                point_wkt = f"POINT({real_estate_latitude} {real_estate_longitude})"

                                real_estate_insert_sql = """
                                    INSERT INTO real_estate (housing_type_id, code, name, type, rent_type, address_detail, coordinate,
                                    deposit, price, description, direction_standard, direction_facing, img_urls, total_floor, target_floor)
                                    VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s)
                                """

                                real_estate_insert_data = (
                                    housing_type_id,
                                    int(estate_overall_data['basicInfo']['atclNo']),
                                    estate_overall_data['basicInfo']['atclNm'],
                                    estate_overall_data['basicInfo']['tradTpNm'],
                                    estate_overall_data['basicInfo']['rletTpCd'],
                                    estate_overall_data['priceInfo']['communalComplexInfo']['dongName'],
                                    point_wkt,
                                    estate_overall_data['priceInfo']['priceInfo']['warrantyAmount'],
                                    estate_overall_data['priceInfo']['priceInfo']['rentAmount'],
                                    estate_overall_data['priceInfo']['detailInfo']['articleDetailInfo'][
                                        'articleFeatureDescription'],
                                    estate_overall_data['priceInfo']['detailInfo']['spaceInfo']['direction'],
                                    estate_overall_data['priceInfo']['detailInfo']['spaceInfo']['directionStandard'],
                                    estate_overall_data['basicInfo']['img'],
                                    estate_overall_data['priceInfo']['detailInfo']['spaceInfo']['floorInfo']['totalFloor'],
                                    estate_overall_data['priceInfo']['detailInfo']['spaceInfo']['floorInfo']['targetFloor']
                                )

                                
                                cursor.execute(real_estate_insert_sql, real_estate_insert_data)
                                connection.commit()
                                print(f"{cursor.lastrowid} 매물 데이터 생성 완료")

                            except Error as e:
                                print(f"Error: {e}")
                            finally:
                                cursor.close()

                            time.sleep(0.5)  # 서버 과부하 방지를 위한 딜레이
    print("모든 구의 매물 데이터를 gu.txt에 저장했습니다.")
    connection.close()