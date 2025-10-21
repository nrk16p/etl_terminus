import requests
import io
import pandas as pd
from datetime import datetime
import random
import time

# ======================================================
# 🚚 Vehicle IDs
# ======================================================
vehicle_ids = [
    5196,5175,6512,6511,6513,27875
    ,27874,48843,5474,14781,4175,8926,
    8928,8927,6424,53407,50524,50523,
    50526,50527,50528,47480,47476,47546,47537
    ,47475,47547,47474,50534,50535,50536,50531,50530,50529,50532,47477,47479,47473,47478,50538,50537,50542,50539,50540,50541,49936,49878,49947,49886,49873,49142,49144,49151,49148,49143,49147,49150,49149,49145,49665,49660,49682,49667,49666,49678,49677,49685,49683,50514,50515,50516,50518,50517,50561,50560,50562,50563,50559,50657,50658,50659,50655,50656,
    2337,2338,6248,26914,53905,5501,26913,49261,49260,49257,49256,49258,49339,49342,49343,49340,49341,49653,49652,49654,49655,49663,49659,49773,49775,49774,
    49772,49790,49789,49791,49788,49943,49940,49941,49945,49942,49944,50708,50717,50709,50711,50710,50713,50712,50714,50716,50715,51407,51408,51410,51411,
    51409,51832,51803,51801,51802,51804,1392,2047,59236,4233,17001,51815,51814,5252,5258,5257,5264,5263,5261,5163,24319,5250,5306,5197,5168,5253,5192,5216,
    4232,5273,5203,5272,5162,5173,5177,43376,5371,5209,5251,5211,5301,5213,5198,5259,5246,5210,5174,5191,5195,5248,5217,5262,5218,5309,5190,5180,5202,5152,
    5164,5199,5215,5194,5161,5193,5214,5212,5179,5310,5167,10093,10156,10175,10094,10095,10176,10177,10178,10179,10180,10182,10181,10283,10284,10285,18607,
    18608,18609,18610,18611,18722,18725,18723,18724,18726,12423,234,377,236,192,206,5165,320,14782,589,43356,930,12425,12427,372,
    326,23834,23837,23835,23838,23836,23882,23847,23881,23880,23848,25299,25298,37634,44069,44073,374,328,228,382,375,203,202,221,43373,204,184,5207,49096,
    49092,49095,49094,49097,49093,49474,49471,49476,49472,49469,49473,49475,49470,50689,50692,50687,50685,50684,50690,50693,50694,50686,50688,51345,51352,51344,
    51349,51346,51351,51347,51350,51454,51455,51456,51459,51460,51458,51453,51457,51694,51692,51688,51691,51622,51620,51690,51693,40794,5172,318,12128,12131,373,
    12129,393,263,2350,5353,5361,1391,5395,5348,5473,5476,5500,8283,8282,6422,5475,10286,5497,8286,10287,6423,384,2347,52821,8284,8285,5472,394,5499,5498,222,4174,
    1096,6421,8930,49333,27873,5241,52776,5208,59158,5242,4173,46070,46068,46069,46071,46121,46072,46096,46094,46095,46114,46122,49072,49068,49069,49071,2692,2351,
    2342,2330,1089,1075,1073,1074,1072,1088,1091,1086,1085,1100,1612,1122,3034,3281,1860,2083,1611,1595,2082,2081,2022,2185,2084,2085,52819,3180,2036,3285,2888,2700,
    2868,2694,2855,2186,2226,6130,5376,5384,5352,5370,5354,5360,5374,5398,5455,5558,5375,5364,5355,5359,5369,5358,5363,316,2339,2345,2837,2331,2357,2343,2341,2340,2346,
    2327,52816,5243,40786,52822,5166,2329,12428,58184,2358,8751,8748,5150,5104,5100,5103,5102,8747,5101,8750,8749,8758,8757,5097,5098,5096,5099,5373,5394,5430,5399,5086,
    54073,5385,5400,5089,5088,5386,5402,5350,5351,5387,5349,5087,5383,5404,5382,5391,5381,5388,5389,5401,5390,5403,6128,9946,9947,9943,9944,9945,10741,10745,10744,10742,
    10743,10852,10849,10850,10851,10848,11002,10999,11000,10998,11001,11154,11153,11150,11151,11152,52791,599,224,5372,52813,52792,205,231,586,584,585,713,600,601,602,233,
    223,39673,319,4172,44840,5200,39986,43375,2328,376,5176,52815,52817,39987,49146,49684,58948,6097,8929,59323,59301,52814,52820,24453,55829,58534,24446,24443,46609,46610,
    24456,24455,24445,24441,24447,24448,24452,24454,24449,24450,24451]

# ======================================================
# 📅 Date range
# ======================================================
from datetime import datetime, timedelta

# ======================================================
# 📅 Date range (auto-yesterday)
# ======================================================
yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
date_start = yesterday
date_end   = yesterday
time_start = "00:00:00"
time_end   = "23:59:59"

# ======================================================
# 🌐 API endpoint
# ======================================================
url = "https://report-mongo.terminusfleet.com/reports/go/drivingdistance"

# ======================================================
# 📦 Payload
# ======================================================
payload = {
    "date_start": date_start,
    "date_end": date_end,
    "time_start": time_start,
    "time_end": time_end,
    "list_vehicle_id": vehicle_ids,
    "company_id": 31,
    "vehicle_type_id": "",
    "user_id": 230,
    "type_file": "excel",
    "vehicle_visibility": (
        "184,192,202,203,204,205,206,221,222,223,224,228,231,233,234,236,"
        "263,316,318,319,320,326,328,372,373,374,375,376,377,382,384,393,394"
    ),
}

# ======================================================
# 🎯 Headers
# ======================================================
user_agents = [
    "Mozilla/5.0 (Linux; Android 11; SM-N985F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; CPH2021) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.66 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_3_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36 Edg/89.0.774.68",
]

headers = {
    "User-Agent": random.choice(user_agents),
    "Referer": "https://app-v2.terminusfleet.com/",
    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/zip,*/*;q=0.8",
}

# ======================================================
# 🚀 Run Once
# ======================================================
print("🚀 Requesting driving distance report for all vehicles...")
start_time = time.time()

try:
    resp = requests.post(url, json=payload, headers=headers, timeout=120)
    resp.raise_for_status()
    result_link = resp.json().get("result")
    if not result_link:
        raise ValueError("No result link found in response.")
    print("✅ Report link received.")
except Exception as e:
    print(f"❌ Error fetching report link: {e}")
    exit(1)

# ======================================================
# 📥 Download & Parse Excel
# ======================================================
try:
    print("📥 Downloading Excel file...")
    r = requests.get(result_link, headers=headers, timeout=180)
    r.raise_for_status()

    # Try to read directly as Excel (skip header rows)
    df = pd.read_excel(io.BytesIO(r.content), engine="openpyxl", skiprows=3)
    print(f"✅ Download complete: {len(df)} rows, {len(df.columns)} columns")

    # ======================================================
    # 🧹 Data Cleaning
    # ======================================================
    # 1️⃣ Drop the last column (often blank or summary)
    # 🧹 1️⃣ Drop the last column

# 🧹 1️⃣ Drop the last column (if blank)
    df = df.iloc[:, :-1]

    # 🧱 2️⃣ Keep the first 2 columns as identifiers
    id_cols = df.columns[:2].tolist()

    # 🧮 3️⃣ Unpivot the daily columns
    df_melt = df.melt(
        id_vars=id_cols,
        var_name="date_raw",
        value_name="distance"
    )

    # 🗓️ 4️⃣ Convert "# 1", "# 2", ... → actual date (YYYY-MM-DD)
    base_date = pd.to_datetime(date_start)

    def to_real_date(day_str):
        try:
            day_num = int(str(day_str).replace('#', '').strip())
            new_date = base_date.replace(day=day_num)
            return new_date.strftime('%Y-%m-%d')
        except Exception:
            return None

    df_melt["date"] = df_melt["date_raw"].apply(to_real_date)
    df_melt = df_melt.drop(columns=["date_raw"])
    df_melt = df_melt.dropna(subset=["date", "distance"])

    # 🏷️ 5️⃣ Rename & reorder columns
    df_melt = df_melt.rename(columns={
        id_cols[0]: "ทะเบียนพาหนะ",
        id_cols[1]: "รหัสพาหนะ"
    })[["ทะเบียนพาหนะ", "รหัสพาหนะ", "date", "distance"]]

    # 💾 Save transformed result
    print("✅ Saved to drivingdistance_daily.xlsx")
    print(df_melt.head(10))

except Exception as e:
    print(f"❌ Error reading or transforming Excel file: {e}")

print(f"⏱️ Total time: {(time.time() - start_time):.1f} seconds")




import pandas as pd
import requests
import json

# ---------------------------------------------------
# 1️⃣  Prepare your DataFrame
# ---------------------------------------------------
# Assuming df_melt is already loaded
# Rename Thai columns to match API fields
df_melt = df_melt.rename(columns={
    'ทะเบียนพาหนะ': 'plate_number',
    'รหัสพาหนะ': 'truck_number'
})

# Add gps_vendor column
df_melt['gps_vendor'] = 'terminus'

# ---------------------------------------------------
# 2️⃣  Convert to JSON list of dicts
# ---------------------------------------------------
records = df_melt.to_dict(orient='records')

# (optional) Save a sample for inspection
with open("drivingdistance_data.json", "w", encoding="utf-8") as f:
    json.dump(records[:10], f, ensure_ascii=False, indent=2)
print(f"✅ Prepared {len(records):,} records for upload.")


from tqdm import tqdm

# ---------------------------------------------------
# 3️⃣  Send data in safe chunks to FastAPI backend
# ---------------------------------------------------
url = "https://be-analytics.onrender.com/drivingdistance/bulk"

# Add headers with x-token
headers = {
    "Content-Type": "application/json",
    "x-token": "FGB+xu?r8qM.q9$2:i"
}

def chunk_list(data, chunk_size=2000):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

print("\n🚀 Uploading data to FastAPI backend...")
for idx, chunk in enumerate(tqdm(list(chunk_list(records, 2000)), desc="Uploading batches"), start=1):
    try:
        r = requests.post(url, headers=headers, json=chunk, timeout=120)
        if r.status_code == 200:
            tqdm.write(f"✅ Batch {idx}: Sent {len(chunk)} records successfully")
        else:
            tqdm.write(f"⚠️ Batch {idx}: Failed ({r.status_code}) → {r.text[:120]}")
    except Exception as e:
        tqdm.write(f"❌ Batch {idx} error: {e}")

print("\n🏁 All batches processed!")
