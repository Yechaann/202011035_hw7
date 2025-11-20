import requests
import pandas as pd
from datetime import datetime, timedelta

SERVICE_KEY = "9ef54157b35522cbef8c85428ed4debe3cdcb67280daffc9490f3632671bb1e7"

STN_ID = "108"

# API
BASE_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"
DATA_CD = "ASOS"
DATE_CD = "HR"


import requests
import pandas as pd

BASE_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"
DATA_CD = "ASOS"
DATE_CD = "HR"

def call_asos_api(start_dt: str, start_hh: str, end_dt: str, end_hh: str,
                  stn_id: str, num_of_rows: int = 100) -> pd.DataFrame:
    """
    기상청 ASOS 시간자료 API 호출
    """

    params = {
        "serviceKey": SERVICE_KEY,
        "dataType": "JSON",
        "dataCd": DATA_CD,
        "dateCd": DATE_CD,
        "startDt": start_dt,
        "startHh": start_hh,
        "endDt": end_dt,
        "endHh": end_hh,
        "stnIds": stn_id,
        "pageNo": "1",
        "numOfRows": str(num_of_rows),
    }

    resp = requests.get(BASE_URL, params=params)

    print("=== HTTP DEBUG ===")
    print("status_code:", resp.status_code)
    print("request URL:", resp.url)
    print("body 앞 300글자:", resp.text[:300])
    print("==================")

    if resp.status_code != 200:
        return pd.DataFrame()

    data = resp.json()
    body = data.get("response", {}).get("body", {})
    items = body.get("items", {}).get("item", [])

    if not items:
        print(f"[경고] 데이터 없음: {start_dt} {start_hh}~{end_dt} {end_hh}, stnId={stn_id}")
        return pd.DataFrame()

    return pd.DataFrame(items)

    params = {
        "serviceKey": SERVICE_KEY,
        "dataType": "JSON",
        "dataCd": DATA_CD,
        "dateCd": DATE_CD,
        "startDt": start_dt,
        "startHh": start_hh,
        "endDt": end_dt,
        "endHh": end_hh,
        "stnIds": stn_id,
        "pageNo": "1",
        "numOfRows": str(num_of_rows),
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()

    # 응답 구조: response -> body -> items -> item (list)
    body = data.get("response", {}).get("body", {})
    items = body.get("items", {}).get("item", [])

    if not items:
        print(f"[경고] 데이터 없음: {start_dt} {start_hh}~{end_dt} {end_hh}, stnId={stn_id}")
        return pd.DataFrame()

    df = pd.DataFrame(items)
    return df


def main():
    range1 = {
        "start_dt": "20241204",
        "start_hh": "15",
        "end_dt": "20241204",
        "end_hh": "18",
    }


    range2 = {
        "start_dt": "20250604",
        "start_hh": "12",
        "end_dt": "20250604",
        "end_hh": "16",
    }

    range3 = {
        "start_dt": "20251118",
        "start_hh": "00",
        "end_dt": "20251118",
        "end_hh": "03",
    }

    ranges = [range1, range2, range3]

    all_df_list = []

    for i, r in enumerate(ranges, start=1):
        print(f"\n=== 구간 {i} 조회 ===")
        print(f"{r['start_dt']} {r['start_hh']}시 ~ {r['end_dt']} {r['end_hh']}시, stnId={STN_ID}")

        df = call_asos_api(
            start_dt=r["start_dt"],
            start_hh=r["start_hh"],
            end_dt=r["end_dt"],
            end_hh=r["end_hh"],
            stn_id=STN_ID,
        )

        if not df.empty:
            df["range_no"] = i
            all_df_list.append(df)

    if not all_df_list:
        print("가져온 데이터가 없습니다. ServiceKey, 날짜/시간, stnIds를 다시 확인하세요.")
        return

    result_df = pd.concat(all_df_list, ignore_index=True)

    print("\n=== 통합 데이터 미리보기(상위 10행) ===")
    print(result_df.head(10))

    csv_name = "asos_result.csv"
    result_df.to_csv(csv_name, index=False, encoding="utf-8-sig")
    print(f"\nCSV 저장 완료: {csv_name}")


if __name__ == "__main__":
    main()