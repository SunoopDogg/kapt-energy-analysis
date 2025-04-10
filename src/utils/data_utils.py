import os
import re
import pandas as pd
from typing import List


def load_csv_data(file_name, source_folder='processed'):
    """
    CSV 파일을 불러와 DataFrame으로 반환

    Args:
        file_name (str): CSV 파일 이름
        source_folder (str): CSV 파일이 위치한 폴더 이름 (기본값: 'processed')

    Returns:
        pandas.DataFrame: 불러온 CSV 데이터

    Raises:
        FileNotFoundError: 파일이 존재하지 않을 경우
    """
    base_path = os.path.join(os.getcwd(), 'data')
    file_path = os.path.join(base_path, source_folder, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

    try:
        return pd.read_csv(file_path, encoding='utf-8-sig', low_memory=False)
    except Exception as e:
        raise IOError(f"CSV 파일 로드 중 오류 발생: {e}")


def save_energy_data_to_csv(data_list, filename, output_folder='energy'):
    """
    에너지 데이터를 CSV 파일로 저장

    Args:
        data_list (list): 저장할 데이터 목록
        filename (str): 저장할 파일명 (확장자 포함)
        output_folder (str): 출력 폴더 이름 (기본값: 'energy')

    Returns:
        str: 저장된 CSV 파일 경로

    Raises:
        ValueError: 데이터가 비어있는 경우
    """
    if not data_list:
        raise ValueError("저장할 데이터가 비어 있습니다")

    base_path = os.path.join(os.getcwd(), 'data')
    output_dir = os.path.join(base_path, output_folder)
    filepath = os.path.join(output_dir, filename)

    # 출력 디렉토리 확인 및 생성
    os.makedirs(output_dir, exist_ok=True)

    # DataFrame을 CSV 파일로 저장
    pd.DataFrame(data_list).to_csv(filepath, mode='a', index=False, encoding='utf-8-sig',
                                   header=not os.path.exists(filepath))

    return filepath


def get_csv_files(directory: str) -> List[str]:
    """
    지정된 디렉토리에서 모든 CSV 파일의 목록을 가져옵니다.

    Args:
        directory: CSV 파일이 있는 디렉토리 경로

    Returns:
        CSV 파일명 목록
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"디렉토리를 찾을 수 없습니다: {directory}")

    return [f for f in os.listdir(directory) if f.lower().endswith('.csv')]


def decoding_file_name(file_name: str) -> tuple:
    """
    파일 이름을 디코딩하여 단지 코드와 단지명을 반환합니다.

    Args:
        file_name: CSV 파일 이름

    Returns:
        tuple: (단지 코드, 단지명)
    """
    # 일반적인 양식: "A숫자_단지명_{상관없음}.csv"
    pattern = r"([A-Z]\d+)_(.+?)(_\w+)?\.csv$"
    match = re.match(pattern, file_name)

    if match:
        apt_code = match.group(1)
        complex_name = match.group(2)
        return apt_code, complex_name
    return None, None


def filter_zero_energy_rows(df: pd.DataFrame, energy_columns: List[str]) -> pd.DataFrame:
    """
    모든 에너지 필드가 0인 행을 제거합니다.

    Args:
        df: 원본 데이터프레임
        energy_columns: 에너지 컬럼 목록

    Returns:
        전처리된 데이터프레임
    """
    # 모든 에너지 필드가 0인 행 제거
    non_zero_mask = (df[energy_columns] != 0).any(axis=1)

    # 경고를 방지하기 위해 .copy()를 사용하여 명시적 복사본 생성
    filtered_df = df[non_zero_mask].copy()

    print(
        f"총 {len(df)}개 행 중 {len(df) - len(filtered_df)}개 행이 제거되었습니다 (모든 에너지 필드가 0).")

    return filtered_df


def preprocess_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    시간 관련 컬럼을 전처리합니다.

    Args:
        df: 원본 데이터프레임

    Returns:
        시간 컬럼이 추가된 데이터프레임
    """
    # 복사본 생성
    result_df = df.copy()

    # 데이터 타입 안전하게 변환
    # requestMonth가 이미 문자열인지 확인하고, 그렇지 않으면 문자열로 변환
    if pd.api.types.is_numeric_dtype(result_df['requestMonth']):
        result_df['requestMonth'] = result_df['requestMonth'].astype(str)

    # year와 month 열 생성
    result_df['year'] = result_df['requestMonth'].str[:4].astype(int)
    result_df['month'] = result_df['requestMonth'].str[-2:].astype(str)

    return result_df


def save_analysis_results(data_list, filename, output_folder='analysis'):
    """
    분석 결과를 CSV 파일로 저장합니다. 이미 파일이 존재하면 덮어씁니다.

    Args:
        data_list: 저장할 데이터 목록
        filename: 저장할 파일명 (확장자 포함)
        output_folder: 출력 폴더 이름 (기본값: 'analysis')
    """
    base_path = os.path.join(os.getcwd(), 'data')
    output_dir = os.path.join(base_path, output_folder)
    filepath = os.path.join(output_dir, filename)

    # 출력 디렉토리 확인 및 생성
    os.makedirs(output_dir, exist_ok=True)

    # DataFrame을 CSV 파일로 저장
    pd.DataFrame(data_list).to_csv(filepath,
                                   index=False, encoding='utf-8-sig')

    return filepath
