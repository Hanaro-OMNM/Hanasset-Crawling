# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import json
import time
import math

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
            gu_filter_data = re.findall('filter: {(.+?)},', str(gu_soup.select('script')[3]), flags=re.DOTALL)
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
                'tradTpCd': 'A1:B1:B2',  # 거래 타입 (A1:매매, B1:전세, B2:월세)
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
                        "lng": item.get("lng")  # 경도 정보
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
                            'basic_info' : property_data,
                            'estate_key_info' : article_filter_data[0]['state']['data']['result'],
                            'image_info' : article_filter_data[1],
                            'price_info' : article_filter_data[2]['state']['data']['result'],
                            'etc_info' : article_filter_data[3]['state']['data']['result'],
                            'address_info' : article_filter_data[4]['state']['data']['result'],
                            'maintenance_info' : article_filter_data[5]['state']['data']['result'],
                            'floor_plan_info' : article_filter_data[6]['state']['data']['result'],
                            'utility_info' : article_filter_data[7]['state']['data']['result'],
                            'broker_info' : article_filter_data[8]['state']['data']['result']
                        }

                    f.write(json.dumps(estate_overall_data, ensure_ascii=False) + "\n")  # JSON 형식으로 저장
                time.sleep(0.5)  # 서버 과부하 방지를 위한 딜레이

print("모든 구의 매물 데이터를 gu.txt에 저장했습니다.")
