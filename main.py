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
                            'basicInfo' : property_data,
                            'estateKeyInfo' : article_filter_data[0]['state']['data']['result'],
                            'priceInfo' : article_filter_data[2]['state']['data']['result'],
                            'addressInfo' : article_filter_data[4]['state']['data']['result'],
                            'maintenanceInfo' : article_filter_data[5]['state']['data']['result'],
                            'floorPlanInfo' : article_filter_data[6]['state']['data']['result'],
                            'brokerInfo' : article_filter_data[8]['state']['data']['result']
                        }

                        # `tradeType` 종류 B1: 전세, B2: 월세
                        trade_types = ["B1", "B2"]
                        all_trade_data = {}

                        for trade_type in trade_types:
                            # `real_price_info` 데이터 리스트 초기화
                            all_real_price_data = []

                            real_price_url = "https://fin.land.naver.com/front-api/v1/complex/pyeong/realPrice"
                            # 새로운 API 호출에 필요한 파라미터 생성
                            real_price_params = {
                                "complexNumber": estate_overall_data['estateKeyInfo']['key']['complexNumber'],
                                "pyeongTypeNumber": estate_overall_data['estateKeyInfo']['key']['pyeongTypeNumber'],
                                "page": 1,
                                "size": 10,
                                "tradeType": trade_type
                            }
                            real_price_response = requests.get(real_price_url, params=real_price_params)
                            
                            if real_price_response.status_code == 200:
                                real_price_data = real_price_response.json()
                                all_real_price_data.extend(real_price_data['result']['list'])


                            # 수집한 데이터를 `all_trade_data`에 저장
                            all_trade_data[trade_type] = all_real_price_data

                        # 수집한 모든 데이터를 `estate_overall_data`에 추가
                        estate_overall_data['realPriceInfo'] = all_trade_data

                        print(estate_overall_data)
                        f.write(json.dumps(estate_overall_data, ensure_ascii=False) + "\n")  # JSON 형식으로 저장
                        time.sleep(0.5)  # 서버 과부하 방지를 위한 딜레이

print("모든 구의 매물 데이터를 gu.txt에 저장했습니다.")
