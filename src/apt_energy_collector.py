import os
import signal
from dotenv import load_dotenv

from api.energy_api import fetch_apt_energy_info
from utils.data_utils import load_csv_data, save_energy_data_to_csv
from utils.date_utils import calculate_req_date, get_monthly_dates

# 상수 정의
CSV_FILENAME = '20250328_단지_기본정보_수도권.csv'
OUTPUT_FOLDER = 'energy'

load_dotenv()

terminate_program = False


def signal_handler(sig, frame):
    global terminate_program
    print("\n프로그램 종료 요청이 감지되었습니다. 현재 작업 완료 후 종료합니다...")
    terminate_program = True


def prepare_apt_info(row):
    kapt_code = row['단지코드']
    apt_name = row['단지명']

    approval_date = row['사용승인일']
    if isinstance(approval_date, float):
        approval_date = str(int(approval_date))

    req_date = calculate_req_date(approval_date)

    return kapt_code, apt_name, approval_date, req_date


def get_target_months(kapt_code, apt_name, start_date, end_date):
    monthly_dates = get_monthly_dates(start_date, end_date)

    filename = f"{kapt_code}_{apt_name}_{start_date}_{end_date}.csv"
    base_path = os.path.join(os.getcwd(), 'data')
    output_path = os.path.join(base_path, OUTPUT_FOLDER, filename)

    if os.path.exists(output_path):
        collected_months = load_csv_data(filename, source_folder=OUTPUT_FOLDER)[
            'requestMonth'].tolist()
        collected_months = [str(month) for month in collected_months]

        target_months = [
            month for month in monthly_dates if month not in collected_months]

        if not target_months:
            print(f"[{apt_name}] 이미 모든 데이터 수집 완료")
            return [], filename

        return target_months, filename

    return monthly_dates, filename


def fetch_energy_data(service_key, kapt_code, apt_name, monthly_dates):
    global terminate_program
    all_results = []

    for req_month in monthly_dates:
        if terminate_program:
            break

        print(f"[{req_month}] [{apt_name}] 요청 중...", end=' ')
        response = fetch_apt_energy_info(service_key, kapt_code, req_month)

        try:
            if response and (response['response']['header']['resultCode'] == '00' or
                             response['response']['header']['resultCode'] == '1'):
                item = {'requestMonth': req_month}
                item.update(response['response']['body']['item'])
                all_results.append(item)
                print(f"요청 완료")
            else:
                print(f"요청 실패")
                print(f"응답 코드: {response['response']['header']['resultCode']}")
                print(f"응답 메시지: {response['response']['header']['resultMsg']}")
                print(f"응답 내용: {response['response']['body']}")
        except Exception as e:
            if isinstance(response, str) and "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in response and "returnReasonCode>22<" in response:
                print("일일 API 요청 한도를 초과했습니다. 프로그램을 종료합니다.")
                terminate_program = True
                break
            print(f"데이터 처리 중 오류 발생: {e}")
            break

    return all_results


def process_apartments(df, service_key):
    for idx, row in df.iterrows():
        if terminate_program:
            break

        kapt_code, apt_name, approval_date, req_date = prepare_apt_info(row)

        if req_date is None:
            print(f"[{apt_name}] 사용승인일이 유효하지 않거나 요청 날짜 계산 실패")
            continue

        start_date, end_date = req_date
        monthly_dates, filename = get_target_months(
            kapt_code, apt_name, start_date, end_date)

        if not monthly_dates:
            continue

        print("\n" + "="*50)
        print(f"[{idx+1}/{len(df)}] API 호출 준비 정보")
        print("-"*50)
        print(f"- 단지 코드: {kapt_code}")
        print(f"- 단지명: {apt_name}")
        print(f"- 사용승인일: {approval_date}")
        print("="*50 + "\n")

        all_results = fetch_energy_data(
            service_key, kapt_code, apt_name, monthly_dates)

        if all_results:
            save_energy_data_to_csv(all_results, filename,
                                    output_folder=OUTPUT_FOLDER)
            print(f"[{apt_name}] 총 {len(all_results)}개월 데이터 수집 완료")

        # break


def main():
    print("프로그램 실행 중... (Ctrl+C를 누르면 프로그램이 종료됩니다)")

    # CSV 데이터 로드
    df = load_csv_data(CSV_FILENAME)

    # 환경 변수에서 서비스 키 로드
    service_key = os.getenv("SERVICE_KEY")
    if not service_key:
        print("SERVICE_KEY 환경 변수가 설정되지 않았습니다.")
        return

    # 아파트 정보 처리
    process_apartments(df, service_key)

    print("프로그램이 종료되었습니다.")


# SIGINT(Ctrl+C)에 대한 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    main()
