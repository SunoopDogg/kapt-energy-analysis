import os
import pandas as pd


def update_metropolitan_data(file_name, source_folder='raw', target_folder='processed',
                             output_suffix='_수도권', metropolitan_regions=None):
    """
    엑셀 파일에서 수도권(서울, 경기, 인천) 데이터만 추출하여 CSV로 저장하는 함수

    Args:
        file_name (str): 처리할 엑셀 파일명 (확장자 포함)
        source_folder (str): 소스 엑셀 파일이 위치한 폴더 경로. 기본값 'xlsx'
        target_folder (str): 결과 CSV 파일을 저장할 폴더 경로. 기본값 'csv'
        output_suffix (str): 출력 파일명에 추가할 접미사. 기본값 '_수도권'
        metropolitan_regions (list): 수도권으로 간주할 지역 목록. 기본값은 ['서울특별시, '경기도', '인천광역시']

    Returns:
        str: 생성된 CSV 파일 경로
    """
    if metropolitan_regions is None:
        metropolitan_regions = ['서울특별시', '경기도', '인천광역시']

    # 파일 경로 설정
    base_path = os.path.join(os.getcwd(), 'data')
    file_path = os.path.join(base_path, source_folder, file_name)

    # 파일 존재 확인
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

    # 대상 폴더 확인 및 생성
    target_path = os.path.join(base_path, target_folder)
    os.makedirs(target_path, exist_ok=True)

    # 엑셀 파일 읽기 (첫 번째 행은 제목)
    df = pd.read_excel(file_path, skiprows=1)

    # 수도권 데이터만 필터링
    df_metropolitan = df[df.iloc[:, 0].isin(metropolitan_regions)]

    # 파일명 생성 (확장자 변경)
    output_file_name = os.path.splitext(
        file_name)[0] + output_suffix + '.csv'
    output_path = os.path.join(target_path, output_file_name)

    # CSV로 저장
    df_metropolitan.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"수도권 데이터가 성공적으로 저장되었습니다: {output_path}")
    return output_path


if __name__ == "__main__":
    file_name = '20250328_단지_기본정보.xlsx'

    try:
        # 수도권 데이터 추출 및 저장
        updated_file = update_metropolitan_data(
            file_name, metropolitan_regions=['서울특별시'])
        print(f"데이터 처리 완료: {updated_file}")
    except Exception as e:
        print(f"오류 발생: {e}")
