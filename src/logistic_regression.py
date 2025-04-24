import os
import numpy as np
import pandas as pd
import statsmodels.api as sm


def extract_columns(file_path, columns=None):
    """
    CSV 파일에서 특정 컬럼만 추출하여 DataFrame으로 반환하는 함수

    Args:
        file_path (str): 처리할 CSV 파일 경로
        columns (list): 추출할 컬럼명 목록

    Returns:
        pd.DataFrame: 지정된 컬럼만 포함된 DataFrame
    """
    # 파일 존재 확인
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

    # CSV 파일 읽기
    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # 지정된 컬럼만 선택
    if columns:
        # 존재하는 컬럼만 필터링
        available_columns = [col for col in columns if col in df.columns]
        if not available_columns:
            raise ValueError(f"지정한 컬럼이 파일에 존재하지 않습니다: {columns}")
        df_selected = df[available_columns]
    else:
        df_selected = df

    return df_selected


# 결측치 제거
def remove_missing_values(df):
    """
    DataFrame에서 결측치를 제거하는 함수

    Args:
        df (pd.DataFrame): 결측치를 제거할 DataFrame

    Returns:
        pd.DataFrame: 결측치가 제거된 DataFrame
    """
    return df.dropna()


def main():
    """
    메인 실행 함수
    """
    """-------------------------"""
    file_name = '20250328_단지_기본정보_수도권'
    file_path = os.path.join(
        os.getcwd(), 'data', 'processed', file_name + '.csv')

    columns_to_extract = ['단지코드', '단지명',
                          '단지분류', '사용승인일', '난방방식', '건물구조', '급수방식']

    df = extract_columns(file_path, columns_to_extract)
    df_cleaned = remove_missing_values(df)
    """-------------------------"""

    """-------------------------"""
    file_name = 'merged_filtered_energy_data'
    file_path = os.path.join(
        os.getcwd(), 'data', 'processed', file_name + '.csv')
    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # energy_type이 waterCool만 추출
    df_waterCool = df[df['energy_type'] == 'waterCool']

    # kapt_code 기준으로 그룹화
    grouped_by_kapt = df_waterCool.groupby('kapt_code')
    """-------------------------"""

    """-------------------------"""
    # waterCool_avg 계산
    waterCool_avg = grouped_by_kapt['correlation'].mean().reset_index()
    waterCool_avg.columns = ['단지코드', 'waterCool_avg']
    waterCool_avg['high_efficiency'] = (
        waterCool_avg['waterCool_avg'] > 0.619).astype(int)
    """-------------------------"""

    # high_efficiency가 1 개수 0 개수 출력
    high_efficiency_count = waterCool_avg['high_efficiency'].value_counts()
    print(f"\nhigh_efficiency 1 개수: {high_efficiency_count[1]}")
    print(f"high_efficiency 0 개수: {high_efficiency_count[0]}")

    # df_cleaned의 '단지코드'와 매칭
    merged_df = pd.merge(df_cleaned, waterCool_avg,
                         left_on='단지코드', right_on='단지코드', how='inner')

    print(f"\n매칭된 데이터 수: {len(merged_df)}")
    print("\n매칭된 데이터 샘플:")
    print(merged_df.head())

    # 로지스틱 회귀분석을 위한 데이터 준비
    # 단지분류 : 도시형 생활주택(연립주택), 도시형 생활주택(주상복합) -> 연립주택, 주상복합
    merged_df['단지분류'] = merged_df['단지분류'].replace(
        {'도시형 생활주택(연립주택)': '연립주택', '도시형 생활주택(주상복합)': '주상복합'})
    # 건물구조: 기타철골철근콘크리트구조 -> 철골철근콘크리트구조, 기타콘크리트구조 -> 콘크리트구조
    merged_df['건물구조'] = merged_df['건물구조'].replace(
        {'기타철골철근콘크리트구조': '철골철근콘크리트구조', '기타콘크리트구조': '콘크리트구조'})
    # 난방방식: 개별난방+기타 제거, 급수방식: 기타 제거
    merged_df = merged_df[~merged_df['난방방식'].isin(['개별난방+기타'])]
    merged_df = merged_df[~merged_df['급수방식'].isin(['기타'])]

    # 로지스틱 회귀 분석
    model = sm.Logit.from_formula(
        'high_efficiency ~ C(단지분류) + C(난방방식) + C(급수방식)', data=merged_df)
    result = model.fit()
    print(result.summary())

    # 승산비
    odds_ratios = np.exp(result.params)
    print("\n승산비:")
    print(odds_ratios)


if __name__ == "__main__":
    main()
