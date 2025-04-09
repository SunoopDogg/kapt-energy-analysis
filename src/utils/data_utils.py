import os
import pandas as pd


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


def save_energy_data_to_csv(data_list, filename, output_folder='output'):
    """
    에너지 데이터를 CSV 파일로 저장

    Args:
        data_list (list): 저장할 데이터 목록
        filename (str): 저장할 파일명 (확장자 포함)
        output_folder (str): 출력 폴더 이름 (기본값: 'output')

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

    # 데이터 목록을 DataFrame으로 변환
    df = pd.DataFrame(data_list)

    # DataFrame을 CSV 파일로 저장
    df.to_csv(filepath, mode='a', index=False, encoding='utf-8-sig',
              header=not os.path.exists(filepath))

    return filepath
