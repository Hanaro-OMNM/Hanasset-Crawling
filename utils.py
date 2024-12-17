import math

def calculate_bounds(lat, lon, zoom):
    R = 6378137  # 지구 반지름 (미터)
    tile_size = 256  # 지도 타일 크기 (픽셀)
    earth_circumference = 2 * math.pi * R  # 지구 둘레

    resolution = earth_circumference / (tile_size * (2 ** zoom))

    lat_rad = math.radians(lat)
    meters_per_degree_lon = 111320 * math.cos(lat_rad)
    meters_per_degree_lat = 110574

    half_width = resolution * tile_size / 2
    half_height = resolution * tile_size / 2

    delta_lat = half_height / meters_per_degree_lat
    delta_lon = half_width / meters_per_degree_lon

    btm = lat - delta_lat
    top = lat + delta_lat
    lft = lon - delta_lon
    rgt = lon + delta_lon

    return {'btm': btm, 'top': top, 'lft': lft, 'rgt': rgt}

def targetFloorUtil(total_floor, target_floor):
    try:
        total_floor = int(total_floor)
    except (ValueError, TypeError):
        return None

    floor_mapping = {
        "고": total_floor // 4 * 3,
        "중": total_floor // 2,
        "저": total_floor // 4
    }

    try:
        return int(target_floor)
    except (ValueError, TypeError):
        return floor_mapping.get(target_floor, None)

def approveDateUtil(date_str):
    # 6자리 (YYYYMM)인 경우
    if len(date_str) == 6:
        return date_str + '01'
    # 4자리 (YYYY)인 경우
    elif len(date_str) == 4:
        return date_str + '0101'
    else:
        return date_str