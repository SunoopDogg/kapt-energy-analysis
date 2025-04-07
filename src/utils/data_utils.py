import os
import pandas as pd


def load_csv_data(file_name, source_folder='csv'):
    """
    CSV 파일을 불러와 DataFrame으로 반환

    Args:
        file_name (str): CSV 파일 이름
        source_folder (str): CSV 파일이 위치한 폴더 이름 (기본값: 'csv')

    Returns:
        pandas.DataFrame: 불러온 CSV 데이터
    """
    base_path = os.getcwd()
    file_path = os.path.join(base_path, source_folder, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

    # CSV 파일 읽기
    return pd.read_csv(file_path, encoding='utf-8-sig', low_memory=False)


def save_energy_data_to_csv(data_list, filename):
    """
    에너지 데이터를 CSV 파일로 저장

    Args:
        data_list (list): 저장할 데이터 목록
        filename (str): 저장할 파일명 (확장자 포함)

    Returns:
        str: 저장된 CSV 파일 경로
    """
    base_path = os.getcwd()
    output_folder = 'output'
    filepath = os.path.join(base_path, output_folder, filename)

    # 출력 디렉토리 확인 및 생성
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # 데이터 목록을 DataFrame으로 변환
    df = pd.DataFrame(data_list)

    # DataFrame을 CSV 파일로 저장
    df.to_csv(filepath, mode='a', index=False, encoding='utf-8-sig',
              header=not os.path.exists(filepath))

    return filepath
