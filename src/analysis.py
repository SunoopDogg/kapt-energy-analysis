import os
import signal
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List

from utils.data_utils import decoding_file_name, filter_zero_energy_rows, get_csv_files, load_csv_data, preprocess_time_columns, save_analysis_results

# 상수 정의
ENERGY_COLUMNS = [
    'heat', 'hheat', 'waterHot', 'hwaterHot', 'gas',
    'hgas', 'elect', 'helect', 'waterCool', 'hwaterCool'
]

ENERGY_NAME_MAPPING = {
    'heat': '난방 사용량',
    'hheat': '난방 사용량',
    'waterHot': '급탕 사용량',
    'hwaterHot': '급탕 사용량',
    'gas': '가스 사용량',
    'hgas': '가스 사용량',
    'elect': '전기 사용량',
    'helect': '전기 사용량',
    'waterCool': '수도 사용량',
    'hwaterCool': '수도 사용량'
}

terminate_program = False


def signal_handler(sig, frame):
    global terminate_program
    print("\n프로그램 종료 요청이 감지되었습니다. 현재 작업 완료 후 종료합니다...")
    terminate_program = True


def analyze_monthly_trend(month_data: pd.DataFrame, column: str) -> Optional[Dict[str, Any]]:
    """
    특정 월의 특정 에너지 유형에 대한 추세를 분석합니다.

    Args:
        month_data: 특정 월의 데이터
        column: 분석할 에너지 컬럼

    Returns:
        분석 결과 딕셔너리 또는 None (분석 불가능한 경우)
    """
    # 결측값 제거
    valid_data = month_data[['year', column]].dropna()
    valid_data = valid_data[valid_data[column] != 0]

    if len(valid_data) < 2:
        return None

    # 상관계수 계산
    correlation = valid_data.corr(method='pearson').iloc[0, 1]

    # 선형 회귀 계수 계산
    x = valid_data['year'].values
    y = valid_data[column].values

    slope, intercept = np.polyfit(x, y, 1)

    # 초기값 및 증가율 계산
    initial_value = intercept + slope * x[0]
    annual_growth_rate = (slope / initial_value) * \
        100 if initial_value != 0 else 0

    # 변화가 미미한 경우 '유지'로 표시하기 위한 임계값 설정
    slope_threshold = 0.001
    if abs(slope) < slope_threshold:
        growth_trend = "유지"
    else:
        growth_trend = "증가" if slope > 0 else "감소"

    return {
        # 시간과 에너지 사용량 간의 상관관계 (-1에서 1 사이 값)
        'correlation': round(correlation, 4),
        'slope': round(slope, 4),  # 선형 회귀선의 기울기 (시간당 에너지 변화량)
        'initial_value': round(initial_value, 2),  # 분석 기간 시작 시점의 예측 에너지 사용량
        'annual_growth_rate': round(annual_growth_rate, 2),  # 연간 증가율 (%)
        'trend': growth_trend,  # 전체적인 추세 (증가/감소/유지)
        'data_points': len(valid_data)  # 분석에 사용된 데이터 포인트 수
    }


def analyze_energy_columns(month: int, month_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    특정 월의 모든 에너지 유형에 대한 분석을 수행합니다.

    Args:
        month: 분석 대상 월
        month_data: 해당 월의 데이터

    Returns:
        각 에너지 유형별 분석 결과 리스트
    """
    results_rows = []
    print(f"\n[{month}] 월별 에너지 사용량 분석 시작")
    print("-"*50)

    for column in ENERGY_COLUMNS:
        if terminate_program:
            break

        print(f"  - {ENERGY_NAME_MAPPING.get(column, column)} 분석 중...", end=' ')
        trend_result = analyze_monthly_trend(month_data, column)

        if trend_result:
            # 결과를 행 형태로 저장
            row_data = {
                'month': month,
                'energy_type': column,
                'energy_name': ENERGY_NAME_MAPPING.get(column, column)
            }
            # 모든 메트릭 추가
            row_data.update(trend_result)
            results_rows.append(row_data)
            print(f"완료 (데이터 포인트: {trend_result['data_points']}개)")
        else:
            print("데이터 부족으로 분석 불가")

    print("-"*50)
    print(f"[{month}] 월별 총 {len(results_rows)}개 에너지 유형 분석 완료")

    return results_rows


def analyze_all_complexes(csv_files: Optional[List[str]] = None):
    """
    output 폴더 내의 모든 CSV 파일을 단지별로 분석합니다.
    각 단지마다 하나의 CSV 파일만 존재합니다.
    """
    global terminate_program

    try:
        for idx, file in enumerate(csv_files):
            if terminate_program:
                break

            df = load_csv_data(file, source_folder='energy')
            kapt_code, complex_name = decoding_file_name(file)

            print("\n" + "="*50)
            print(f"[{idx+1}/{len(csv_files)}] {complex_name} 분석 준비")
            print("-"*50)
            print(f"- 단지 코드: {kapt_code}")
            print(f"- 단지명: {complex_name}")
            print("="*50 + "\n")

            # 데이터 전처리
            df = filter_zero_energy_rows(df, ENERGY_COLUMNS)
            df = preprocess_time_columns(df)
            df = df.reset_index(drop=True)
            month_data = df.groupby('month')

            results = []
            for month, month_df in month_data:
                # 월별 데이터 분석
                monthly_results = analyze_energy_columns(month, month_df)
                results.extend(monthly_results)
                if terminate_program:
                    break

            if results:
                save_analysis_results(
                    results, f"{kapt_code}_{complex_name}_analysis.csv")

            # break

    except Exception as e:
        print(f"분석 중 오류가 발생했습니다: {str(e)}")


def main():
    print("에너지 사용량 분석 프로그램 실행 중... (Ctrl+C를 누르면 프로그램이 종료됩니다)")

    # 모든 CSV 파일 가져오기
    all_csv_files = get_csv_files(os.path.join(os.getcwd(), 'data', 'energy'))

    analyze_all_complexes(all_csv_files)

    print("프로그램이 종료되었습니다.")


# SIGINT(Ctrl+C)에 대한 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n예상치 못한 오류가 발생했습니다: {str(e)}")
