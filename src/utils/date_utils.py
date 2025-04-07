from datetime import datetime


def calculate_req_date(approval_date):
    """
    사용승인일을 기반으로 요청 날짜(reqDate) 계산

    시작일은 사용승인일 기준, 종료일은 현재 기준 이전 달

    Args:
        approval_date (str): 사용승인일 (YYYYMMDD 형식)

    Returns:
        tuple: (시작일(YYYYMM), 종료일(YYYYMM)) 또는 오류 발생 시 None
    """
    current_date = datetime.now()

    # 이전 달 계산
    if current_date.month == 1:
        prev_month = 12
        prev_year = current_date.year - 1
    else:
        prev_month = current_date.month - 1
        prev_year = current_date.year

    # 종료일은 이전 달
    end_date = f"{prev_year}{prev_month:02d}"

    try:
        # 사용승인일 문자열을 날짜 객체로 변환
        approval_date = datetime.strptime(approval_date, '%Y%m%d')

        # 시작일은 사용승인월
        start_date = f"{approval_date.year}{approval_date.month:02d}"

        return start_date, end_date
    except ValueError as e:
        print(f"사용승인일 변환 오류: {e}")
        return None


def get_monthly_dates(start_date, end_date):
    """
    시작일과 종료일 사이의 모든 월(YYYYMM) 반환

    Args:
        start_date (str): 시작 년월 (YYYYMM 형식)
        end_date (str): 종료 년월 (YYYYMM 형식)

    Returns:
        list: YYYYMM 형식의 월 목록
    """
    start = datetime.strptime(start_date, '%Y%m')
    end = datetime.strptime(end_date, '%Y%m')

    result = []
    current = start

    while current <= end:
        result.append(current.strftime('%Y%m'))
        # 다음 달로 이동
        year = current.year + (current.month // 12)
        month = (current.month % 12) + 1
        current = datetime(year, month, 1)

    return result
