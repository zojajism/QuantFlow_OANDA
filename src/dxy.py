import requests
from datetime import datetime, timezone

def get_dxy_value():
    """
    Fetches the latest 1-minute DXY (US Dollar Index) close value from Yahoo Finance.
    Returns:
        float: latest close value (e.g. 106.23)
        None : if request or data parsing fails
    """
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB"
        params = {"interval": "1m", "range": "1d"}
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()

        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        closes = result["indicators"]["quote"][0].get("close", [])

        if not timestamps or not closes:
            return None  # no data

        last_close = closes[-1]
        last_time = datetime.fromtimestamp(timestamps[-1], tz=timezone.utc)

        # Optionally print or log timestamp
        # print(f"DXY (1m) @ {last_time.isoformat()} = {last_close}")

        return float(last_close)

    except Exception as e:
        print(f"[get_dxy_value] Error: {e}")
        return None
