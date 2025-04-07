import os
import signal
from dotenv import load_dotenv

from api.energy_api import fetch_apt_energy_info
from utils.data_utils import load_csv_data, save_energy_data_to_csv
from utils.date_utils import calculate_req_date, get_monthly_dates

load_dotenv()

terminate_program = False


def signal_handler(sig, frame):
    global terminate_program
    print("\n프로그램 종료 요청이 감지되었습니다. 현재 작업 완료 후 종료합니다...")
    terminate_program = True


# SIGINT(Ctrl+C)에 대한 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    print("프로그램 실행 중... (Ctrl+C를 누르면 프로그램이 종료됩니다)")
    file_name = '20250328_단지_기본정보_수도권.csv'
    df = load_csv_data(file_name)

    service_key = os.getenv("SERVICE_KEY")

    # 모든 단지에 대한 처리
    for idx, row in df.iterrows():
        if terminate_program:
            break

        kapt_code = row['단지코드']
        apt_name = row['단지명']

        approval_date = row['사용승인일']
        if isinstance(approval_date, float):
            approval_date = str(int(approval_date))

        req_date = calculate_req_date(approval_date)
        if req_date is None:
            print(f"[{apt_name}] 사용승인일이 유효하지 않거나 요청 날짜 계산 실패")
            continue
        else:
            start_date, end_date = req_date

        monthly_dates = get_monthly_dates(start_date, end_date)

        filename = f"{kapt_code}_{apt_name}_{start_date}_{end_date}.csv"
        output_path = os.path.join('output', filename)

        if os.path.exists(output_path):
            collected_months = load_csv_data(filename, source_folder='output')[
                'requestMonth'].tolist()
            collected_months = [str(month) for month in collected_months]

            target_months = [
                month for month in monthly_dates if month not in collected_months]

            if not target_months:
                print(f"[{apt_name}] 이미 모든 데이터 수집 완료")
                continue

            monthly_dates = target_months

        print("\n" + "="*50)
        print(f"[{idx+1}/{len(df)}] API 호출 준비 정보")
        print("-"*50)
        print(f"- 단지 코드: {kapt_code}")
        print(f"- 단지명: {apt_name}")
        print(f"- 사용승인일: {approval_date}")
        print("="*50 + "\n")

        all_results = []
        for req_month in monthly_dates:
            if terminate_program:
                break

            print(f"[{req_month}] [{apt_name}] 요청 중...", end=' ')
            response = fetch_apt_energy_info(
                service_key, kapt_code, req_month)

            try:
                if response and (response['response']['header']['resultCode'] == '00' or response['response']['header']['resultCode'] == '1'):
                    item = {'requestMonth': req_month}
                    item.update(response['response']['body']['item'])
                    all_results.append(item)
                    print(f"요청 완료")
                else:
                    print(f"요청 실패")
                    print(
                        f"응답 코드: {response['response']['header']['resultCode']}")
                    print(
                        f"응답 메시지: {response['response']['header']['resultMsg']}")
                    print(
                        f"응답 내용: {response['response']['body']}")
            except Exception as e:
                if "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in response and "returnReasonCode>22<" in response:
                    print("일일 API 요청 한도를 초과했습니다. 프로그램을 종료합니다.")
                    terminate_program = True
                    break
                break

        if all_results:
            save_energy_data_to_csv(all_results, filename)
            print(f"[{apt_name}] 총 {len(all_results)}개월 데이터 수집 완료")

    print("프로그램이 종료되었습니다.")
