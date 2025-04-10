import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

from utils.data_utils import decoding_file_name, get_csv_files, load_csv_data


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


def merge_filtered_energy_data(analysis_files: list, output_folder: str):
    """
    분석 파일을 읽고 data_points 값이 5개 이상인 데이터만 병합하여 시각화 준비

    Args:
        analysis_files: 분석 결과 파일 목록
        output_folder: 시각화 결과 저장 폴더

    Returns:
        통합된 데이터프레임
    """
    try:
        all_data = pd.DataFrame()

        for file in analysis_files:
            kapt_code, complex_name = decoding_file_name(file)

            df = load_csv_data(file, source_folder='analysis')

            # data_points 컬럼 값이 5 이상인 데이터만 필터링
            filtered_df = df[df['data_points'] >= 5].copy()  # 명시적으로 복사본 생성

            if not filtered_df.empty:
                # kapt_code 컬럼 추가 (loc를 사용하여 안전하게 값 설정)
                filtered_df.loc[:, 'kapt_code'] = kapt_code
                filtered_df.loc[:, 'complex_name'] = complex_name

                # 통합 데이터프레임에 추가
                all_data = pd.concat(
                    [all_data, filtered_df], ignore_index=True)

        print(
            f"총 {len(all_data)} 행, data_points가 5 이상인 데이터만 병합했습니다.")

        return all_data

    except Exception as e:
        print(f"데이터 병합 중 오류 발생: {e}")
        return pd.DataFrame()


def group_data_by_energy_type_and_month(data: pd.DataFrame):
    """
    데이터를 energy_type과 month로 그룹화합니다.

    Args:
        data: 병합된 데이터프레임

    Returns:
        그룹화된 데이터 딕셔너리
    """
    grouped_data = {}

    # energy_type으로 첫 번째 그룹화
    for energy_type, energy_df in data.groupby('energy_type'):
        grouped_data[energy_type] = {}

        # 각 energy_type 내에서 month로 두 번째 그룹화
        for month, month_df in energy_df.groupby('month'):
            grouped_data[energy_type][month] = month_df

    return grouped_data


def display_grouped_data_info(grouped_data):
    """
    그룹화된 데이터의 정보를 출력합니다.

    Args:
        grouped_data: energy_type과 month로 그룹화된 데이터 딕셔너리
    """
    print("\n====== 그룹화된 데이터 정보 ======")

    for energy_type, months_data in grouped_data.items():
        print(f"\n에너지 타입: {energy_type}")

        for month, df in months_data.items():
            print(f"  월: {month}, 데이터 수: {len(df)}")


def visualize_correlation_by_energy_and_month(grouped_data, output_folder):
    """
    energy_type과 month별로 correlation 분포를 시각화합니다.

    Args:
        grouped_data: energy_type과 month로 그룹화된 데이터 딕셔너리
        output_folder: 시각화 결과 저장 폴더
    """
    print("\n에너지 타입과 월별 상관관계(correlation) 시각화 중...")

    # 1. 박스플롯: 각 에너지 타입별 월별 correlation 분포
    for energy_type, months_data in grouped_data.items():
        plt.figure(figsize=(15, 8))

        # 월별 데이터 수집
        all_months_data = []
        month_labels = []

        for month in sorted(months_data.keys()):
            all_months_data.append(months_data[month]['correlation'].values)
            month_labels.append(str(month))

        # 박스플롯 생성
        plt.boxplot(all_months_data, labels=month_labels)
        plt.title(f'{energy_type} - 월별 상관관계(correlation) 분포', fontsize=16)
        plt.xlabel('월', fontsize=14)
        plt.ylabel('상관관계(Correlation)', fontsize=14)
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        # 저장 및 닫기
        output_path = os.path.join(
            output_folder, f'{energy_type}_correlation_boxplot.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  - {energy_type} 박스플롯 저장 완료: {output_path}")

    # 2. 히트맵: 에너지 타입과 월별 correlation 중앙값
    # 데이터 준비
    heatmap_data = pd.DataFrame()

    for energy_type, months_data in grouped_data.items():
        median_values = {}

        for month, df in months_data.items():
            median_values[month] = df['correlation'].median()

        heatmap_data[energy_type] = pd.Series(median_values)

    # 데이터가 존재하는 경우에만 히트맵 생성
    if not heatmap_data.empty:
        plt.figure(figsize=(12, 10))
        sns.heatmap(heatmap_data.T, annot=True, cmap='coolwarm', center=0,
                    cbar_kws={'label': '상관관계(Correlation) 중앙값'})
        plt.title('에너지 타입과 월별 상관관계 중앙값', fontsize=16)
        plt.xlabel('월', fontsize=14)
        plt.ylabel('에너지 타입', fontsize=14)

        # 저장 및 닫기
        output_path = os.path.join(
            output_folder, 'energy_month_correlation_heatmap.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  - 히트맵 저장 완료: {output_path}")

    # 3. 바플롯: 각 에너지 타입별 평균 상관관계
    plt.figure(figsize=(14, 8))

    # 에너지 타입별 평균 상관관계 계산
    energy_avg_corr = {}
    for energy_type, months_data in grouped_data.items():
        all_correlations = []
        for month, df in months_data.items():
            all_correlations.extend(df['correlation'].values)
        energy_avg_corr[energy_type] = np.mean(all_correlations)

    # 색상 선택 (상관관계 값에 따라)
    colors = ['g' if val >= 0 else 'r' for val in energy_avg_corr.values()]

    # 바플롯 그리기
    plt.bar(energy_avg_corr.keys(), energy_avg_corr.values(), color=colors)
    plt.axhline(y=0, color='k', linestyle='-', alpha=0.3)
    plt.title('에너지 타입별 평균 상관관계', fontsize=16)
    plt.xlabel('에너지 타입', fontsize=14)
    plt.ylabel('평균 상관관계(Correlation)', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # 값 표시
    for i, (key, value) in enumerate(energy_avg_corr.items()):
        plt.text(i, value + (0.02 if value >= 0 else -0.08),
                 f'{value:.3f}', ha='center', fontsize=11)

    # 저장 및 닫기
    output_path = os.path.join(
        output_folder, 'energy_type_avg_correlation.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  - 에너지 타입별 평균 상관관계 바플롯 저장 완료: {output_path}")

    print("상관관계 시각화 완료!")


def main():
    print("에너지 사용량 분석 결과 시각화 시작...")

    # 분석 결과와 시각화 결과 폴더 경로
    analysis_folder = os.path.join(os.getcwd(), 'data', 'analysis')
    analysis_files = get_csv_files(analysis_folder)

    visualization_folder = os.path.join(os.getcwd(), 'data', 'visualization')
    os.makedirs(visualization_folder, exist_ok=True)

    # 데이터 포인트가 5개 이상인 분석 결과만 병합
    merged_data = merge_filtered_energy_data(
        analysis_files, visualization_folder)

    if not merged_data.empty:
        print("데이터 병합 완료!")
        print(f"병합된 데이터 형태: {merged_data.shape}")
        print(f"컬럼 목록: {merged_data.columns.tolist()}")

        # 데이터를 energy_type과 month로 그룹화
        grouped_data = group_data_by_energy_type_and_month(merged_data)

        # 그룹화된 데이터 정보 출력
        # display_grouped_data_info(grouped_data)

        # correlation 시각화
        visualize_correlation_by_energy_and_month(
            grouped_data, visualization_folder)
    else:
        print("데이터 병합 실패 또는 조건에 맞는 데이터 없음")


if __name__ == "__main__":
    main()
