# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import json
import time

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
        url = "https://m.land.naver.com/search/result/" + gu
        response = requests.get(url, headers=header)

        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')

            # 위치 정보를 필터 데이터에서 추출
            filter_data = re.findall('filter: {(.+?)},', str(soup.select('script')[3]), flags=re.DOTALL)
            location_data = {}
            try:
                # 필터 데이터를 위치 정보로 변환
                filter_parts = filter_data[0].split()
                for j in range(len(filter_parts)):
                    if j % 2 == 0:
                        location_data[filter_parts[j].strip(":")] = filter_parts[j + 1].strip(',').strip("'")
            except IndexError:
                pass

            # 매물 데이터 요청에 필요한 위치 정보 추출
            lat = location_data.get('lat', '')
            lon = location_data.get('lon', '')
            cortarNo = location_data.get('cortarNo', '')

            # 매물 데이터 요청
            param = {
                'lat': lat,
                'lon': lon,
                'cortarNo': cortarNo,
                'rletTpCd': 'APT',  # 매물 타입 (APT: 아파트)
                'tradTpCd': 'A1:B1:B2',  # 거래 타입 (A1:매매, B1:전세, B2:월세)
                'z': '12',  # 지도 줌 레벨
                'sort': 'rank'
            }

            page = 0
            while True:
                page += 1
                param['page'] = page

                resp = requests.get(property_url, params=param, headers=header)
                if resp.status_code != 200:
                    print(f'Error fetching data for {gu}, page {page}')
                    break

                data = resp.json()
                results = data.get('body', [])
                if not results:
                    break  # 매물이 없으면 루프 종료

                # 매물 데이터 요청과 저장 루프 안에서
                print(results)
                for item in results:
                    property_data = {
                        "gu": gu,
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
                    f.write(json.dumps(property_data, ensure_ascii=False) + "\n")  # JSON 형식으로 저장
                time.sleep(1)  # 서버 과부하 방지를 위한 딜레이

print("모든 구의 매물 데이터를 gu.txt에 저장했습니다.")
