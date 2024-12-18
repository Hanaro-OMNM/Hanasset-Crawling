from utils import approveDateUtil, calculate_bounds, targetFloorUtil

def find_or_insert_housing_complex(cursor, estate_data):
    """
    housing_complex 테이블의 존재 여부를 확인하고 없으면 삽입 후 ID 반환.
    """
    find_query = "SELECT housing_complex_id FROM housing_complex WHERE code = %s"
    cursor.execute(find_query, (int(estate_data['estateKeyInfo']['key']['complexNumber']),))
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        find_area_query = "SELECT area_code_id FROM area_code WHERE code = %s"
        cursor.execute(find_area_query, (int(estate_data['basicInfo']['cortarNo']),))
        area_code_id = cursor.fetchone()

        insert_query = """
            INSERT INTO housing_complex(
                area_code_id, code, name, address, unit_count, established_date,
                system_type, energy_type, parking_count, dong_count, floor_area_ratio, 
                building_coverage_ratio, construction_company
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_data = (
            area_code_id[0],
            estate_data['estateKeyInfo']['key'].get('complexNumber', None),
            estate_data['basicInfo']['atclNm'],
            estate_data['addressInfo']['address']['legalDivision'] + ' ' + estate_data['addressInfo']['address']['roadName'],
            estate_data['addressInfo']['totalHouseholdNumber'],
            approveDateUtil(estate_data['addressInfo']['useApprovalDate']),
            estate_data['addressInfo']['heatingAndCoolingInfo']['heatingAndCoolingSystemType'],
            estate_data['addressInfo']['heatingAndCoolingInfo']['heatingEnergyType'],
            estate_data['addressInfo']['parkingInfo']['totalParkingCount'],
            estate_data['addressInfo']['dongCount'],
            estate_data['addressInfo']['buildingRatioInfo']['floorAreaRatio'],
            estate_data['addressInfo']['buildingRatioInfo']['buildingCoverageRatio'],
            estate_data['addressInfo']['constructionCompany'],
        )
        cursor.execute(insert_query, insert_data)
        return cursor.lastrowid


def find_or_insert_housing_type(cursor, housing_complex_id, pyeong_data, estate_data):
    find_query = "SELECT housing_type_id FROM housing_type WHERE housing_complex_id = %s AND code = %s"
    cursor.execute(find_query, (housing_complex_id, pyeong_data['result']['number']))
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        floor_plan_urls = ''
        if len(pyeong_data['result']['floorPlanUrls']) > 0:
            base_urls = pyeong_data['result']['floorPlanUrls'].get('BASE', {}).get('0', [])
            floor_plan_urls = base_urls[0] if len(base_urls) == 1 else base_urls[2] if base_urls else ''

        insert_query = """
            INSERT INTO housing_type(
                housing_complex_id, code, name, unit_count, entrance_type, supply_area_size,
                exclusive_area_size, management_fee, room_count, bathroom_count, floor_plan_img_url, floor_plan_link
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_data = (
            housing_complex_id,
            pyeong_data['result'].get('number', None),
            pyeong_data['result'].get('name', ''),
            pyeong_data['result'].get('unitsOfSameArea', None),
            pyeong_data['result'].get('entranceType', ''),
            pyeong_data['result'].get('supplyArea', None),
            pyeong_data['result'].get('exclusiveArea', None),
            estate_data['maintenanceInfo'].get('yearMonthFee', None),
            pyeong_data['result'].get('roomCount', None),
            pyeong_data['result'].get('bathRoomCount', None),
            floor_plan_urls,
            f"https://fin.land.naver.com/complexes/{estate_data['estateKeyInfo']['key']['complexNumber']}?tab=complex-info",
        )
        cursor.execute(insert_query, insert_data)
        return cursor.lastrowid


def insert_real_estate(cursor, housing_type_id, estate_data):
    point_wkt = f"POINT({estate_data['basicInfo']['lat']} {estate_data['basicInfo']['lng']})"
    target_floor = targetFloorUtil(
        estate_data['priceInfo']['detailInfo']['spaceInfo']['floorInfo'].get('totalFloor', None),
        estate_data['priceInfo']['detailInfo']['spaceInfo']['floorInfo'].get('targetFloor', None)
    )

    insert_query = """
        INSERT INTO real_estate (
            housing_type_id, code, name, type, rent_type, address_detail, coordinate,
            deposit, price, description, direction_standard, direction_facing, img_url, total_floor, target_floor
        ) VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_data = (
        housing_type_id,
        int(estate_data['basicInfo']['atclNo']),
        estate_data['basicInfo']['atclNm'],
        estate_data['basicInfo']['tradTpNm'],
        estate_data['basicInfo']['rletTpCd'],
        estate_data['priceInfo']['communalComplexInfo']['dongName'],
        point_wkt,
        estate_data['priceInfo']['priceInfo']['warrantyAmount'],
        estate_data['priceInfo']['priceInfo']['rentAmount'],
        estate_data['priceInfo']['detailInfo']['articleDetailInfo']['articleFeatureDescription'],
        estate_data['priceInfo']['detailInfo']['spaceInfo']['direction'],
        estate_data['priceInfo']['detailInfo']['spaceInfo']['directionStandard'],
        estate_data['basicInfo']['img'],
        estate_data['priceInfo']['detailInfo']['spaceInfo']['floorInfo'].get('totalFloor', None),
        target_floor
    )
    cursor.execute(insert_query, insert_data)
