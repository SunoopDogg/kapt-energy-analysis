import requests


def fetch_apt_energy_info(service_key, kapt_code, req_month):
    """
    공동주택 에너지 사용량 API 호출 함수

    Parameters:
    - service_key: API 서비스 키
    - kapt_code: 단지 코드
    - req_month: 요청 월(YYYYMM)

    Returns:
    - API 응답 데이터(딕셔너리)
    """
    base_url = "http://apis.data.go.kr/1613000/ApHusEnergyUseInfoOfferServiceV2/getHsmpApHusUsgQtyInfoSearchV2"

    params = {
        'serviceKey': service_key,
        'kaptCode': kapt_code,
        'reqDate': req_month,
    }

    try:
        response = requests.get(base_url, params=params)
        return response.json()
    except requests.exceptions.RequestException as e:
        return response.text
