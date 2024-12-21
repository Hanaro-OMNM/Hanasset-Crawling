import requests
import re
from bs4 import BeautifulSoup
from utils import calculate_bounds
import json

def fetch_location_data(gu, header):
    gu_url = f"https://m.land.naver.com/search/result/{gu}"
    response = requests.get(gu_url, headers=header)
    if response.status_code != 200:
        return None
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    return extract_location_data(soup)

def extract_location_data(soup):
    data = {}
    try:
        filter_data = re.findall('filter: {(.+?)},', str(soup.select('script')[4]), flags=re.DOTALL)[0]
        parts = filter_data.split()
        for i in range(0, len(parts), 2):
            data[parts[i].strip(":")] = parts[i + 1].strip(',').strip("'")
    except IndexError:
        pass
    return data

def fetch_property_data(data, page, header):
    # 매물 데이터를 요청하는 URL
    property_url = "https://m.land.naver.com/cluster/ajax/articleList"

    gu_lat = data.get('lat', '')
    gu_lon = data.get('lon', '')

    # 매물 데이터 요청
    gu_param = {
        'lat': gu_lat,
        'lon': gu_lon,
        'btm': calculate_bounds(float(gu_lat), float(gu_lon), 13).get('btm'),
        'top': calculate_bounds(float(gu_lat), float(gu_lon), 13).get('top'),
        'lft': calculate_bounds(float(gu_lat), float(gu_lon), 13).get('lft'),
        'rgt': calculate_bounds(float(gu_lat), float(gu_lon), 13).get('rgt'),
        'cortarNo': data.get('cortarNo', ''),
        'rletTpCd': 'APT',  # 매물 타입 (APT: 아파트)
        'tradTpCd': 'B1:B2',  # 거래 타입 (B1:전세, B2:월세)
        'z': '13',  # 지도 줌 레벨
        'sort': 'rank',
        'page': page
    }

    response = requests.get(property_url, params=gu_param, headers=header)
    if response.status_code != 200:
        return None

    return response.json().get('body', [])

def fetch_article_filter_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        return None

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    return json.loads(str(soup.select('script')[37].string))['props']['pageProps']['dehydratedState']['queries']

