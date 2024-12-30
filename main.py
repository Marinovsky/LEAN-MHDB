from time import timezone
import pytz

import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import OrderedDict
import requests
import json

class market_hours_database:
    def __init__(self):
        self.mhdb = self.get_mhdb_entries()
        self.cme_group_futures_info = self.get_cme_group_future_info_from_local()
        self._cme_equities_filename = "cme_equities.xlsx"
        self._cme_interest_rate_filename = "cme_interest_rate.xlsx"
        self._cme_fx_filename = "cme_fx.xlsx"
        self._cme_crypto_filename = "cme_crypto.xlsx"
        self._cme_energy_filename = "cme_energy.xlsx"
        self._cme_metals_filename = "cme_metals.xlsx"
        self._cme_grains_filename = "cme_grains.xlsx"
        self._cme_lumber_filename = "cme_lumber.xlsx"
        self._cme_livestock_filename = "cme_livestock.xlsx"
        self._cme_dairy_filename = "cme_dairy.xlsx"

    def get_mhdb_entries(self):
        url = "https://raw.githubusercontent.com/Marinovsky/Lean/master/Data/market-hours/market-hours-database.json"
        file = requests.get(url)
        data = json.loads(file.content, object_pairs_hook=OrderedDict)
        return data

    def get_cme_group_future_info_from_cloud(self):
        url = "https://dl.dropboxusercontent.com/scl/fi/09j95smm8ko09aupx66gl/cme-group-futures-info.json?rlkey=43ne4e093vtsqo6o2vnbrvtfh&dl=0"
        df = pd.read_json(url)
        return df

    def get_cme_group_future_info_from_local(self):
        df = pd.read_json("cme-group-futures-info.json")
        return df

    def get_cme_equities_keys(self):
        return self._get_cme_keys(self._cme_equities_filename)

    def get_cme_interest_rate_keys(self):
        return self._get_cme_keys(self._cme_interest_rate_filename)

    def get_cme_fx_keys(self):
        return self._get_cme_keys(self._cme_fx_filename)

    def get_cme_crypto_keys(self):
        return self._get_cme_keys(self._cme_crypto_filename)

    def get_cme_energy_keys(self):
        return self._get_cme_keys(self._cme_energy_filename)

    def get_cme_metals_keys(self):
        return self._get_cme_keys(self._cme_metals_filename)

    def get_cme_grains_keys(self):
        return self._get_cme_keys(self._cme_grains_filename)

    def get_cme_lumber_keys(self):
        return self._get_cme_keys(self._cme_lumber_filename)

    def get_cme_livestock_keys(self):
        return self._get_cme_keys(self._cme_livestock_filename)

    def get_cme_dairy_keys(self):
        return self._get_cme_keys(self._cme_dairy_filename)

    def _get_cme_keys(self, filename):
        df = pd.read_excel(filename)
        merge_columns_function = lambda row: f"\"{str(row['Unnamed: 2'])}\": \"{str(row['Unnamed: 5']).lower()}\""
        symbols_and_markets_df = df.apply(merge_columns_function, axis=1)
        symbols_and_markets_list = [entry for entry in symbols_and_markets_df if ("nan" not in entry and "Globex" not in entry)]
        symbols_and_markets_list = list(set(symbols_and_markets_list))
        return symbols_and_markets_list

    def get_mhdb_key(self, ticker, market):
        return f"Future-{market}-{ticker}"

    def update_late_opens(self, cme_class):
        late_opens = self.cme_group_futures_info[cme_class]["lateOpens"]
        for late_open in late_opens.keys():
            date = datetime.strptime(late_open + " " + late_opens[late_open], "%m/%d/%Y %H:%M:%S")
            self.add_late_open_to_mhdb(cme_class, date)

    def update_early_closes(self, cme_class):
        early_closes = self.cme_group_futures_info[cme_class]["earlyCloses"]
        for early_close in early_closes.keys():
            date = datetime.strptime(early_close + " " + early_closes[early_close], "%m/%d/%Y %H:%M:%S")
            self.add_early_close_to_mhdb(cme_class, date)

    def add_late_open_to_mhdb(self, cme_class, late_open_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            timezone = self.mhdb["entries"][key]["exchangeTimeZone"]
            from_timezone = pytz.timezone("America/Chicago")
            late_open_date_tz = from_timezone.localize(late_open_date)
            date = late_open_date.strftime("%#m/%#d/%Y")
            parsed_hour = late_open_date_tz.astimezone(pytz.timezone(timezone)).strftime("%H:%M:%S")
            if "lateOpens" not in self.mhdb["entries"][key].keys():
                self.mhdb["entries"][key]["lateOpens"] = dict()
            if date not in self.mhdb["entries"][key]["lateOpens"].keys():
                print(f"Late opens {date}:{parsed_hour} not found in MHDB {key} entry")
                self.mhdb["entries"][key]["lateOpens"][date] = parsed_hour
                self.mhdb["entries"][key]["lateOpens"] = dict(sorted(self.mhdb["entries"][key]["lateOpens"].items(), key=lambda d: datetime.strptime(d[0], '%m/%d/%Y')))

    def add_early_close_to_mhdb(self, cme_class, early_close_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            from_timezone = pytz.timezone("America/Chicago")
            early_close_date_tz = from_timezone.localize(early_close_date)
            timezone = self.mhdb["entries"][key]["exchangeTimeZone"]
            date = early_close_date.strftime("%#m/%#d/%Y")
            parsed_hour = early_close_date_tz.astimezone(pytz.timezone(timezone)).strftime("%H:%M:%S")
            if "earlyCloses" not in self.mhdb["entries"][key].keys():
                self.mhdb["entries"][key]["earlyCloses"] = dict()
            if date not in self.mhdb["entries"][key]["earlyCloses"].keys():
                print(f"Early close {date}:{parsed_hour} not found in MHDB {key} entry")
                self.mhdb["entries"][key]["earlyCloses"][date] = parsed_hour
                self.mhdb["entries"][key]["earlyCloses"] = dict(sorted(self.mhdb["entries"][key]["earlyCloses"].items(), key=lambda d: datetime.strptime(d[0], '%m/%d/%Y')))

    def add_holiday_to_mhdb(self, cme_class, holiday_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            date = holiday_date.strftime("%#m/%#d/%Y")
            key = self.get_mhdb_key(product, products[product])
            if (key in self.mhdb["entries"].keys()) and (date not in self.mhdb["entries"][key]["holidays"]):
                print(f"Holiday {date} not found in MHDB {key} entry")
                self.mhdb["entries"][key]["holidays"].append(date)
                self.mhdb["entries"][key]["holidays"] = sorted(self.mhdb["entries"][key]["holidays"], key=lambda d: datetime.strptime(d, '%m/%d/%Y'))

    def add_late_open_to_cme_group_futures_info(self, cme_class, late_open_date):
        entry = self.cme_group_futures_info[cme_class]
        cme_group_futures_info_timezone = entry["exchangeTimeZone"]
        from_timezone = pytz.timezone("America/Chicago")
        date = late_open_date.strftime("%#m/%#d/%Y")
        late_open_date_tz = from_timezone.localize(late_open_date)
        self.cme_group_futures_info[cme_class]["lateOpens"][date] = late_open_date_tz.astimezone(
            pytz.timezone(cme_group_futures_info_timezone)).strftime("%H:%M:%S")

    def add_early_close_to_cme_group_futures_info(self, cme_class, early_close_date):
        entry = self.cme_group_futures_info[cme_class]
        cme_group_futures_info_timezone = entry["exchangeTimeZone"]
        from_timezone = pytz.timezone("America/Chicago")
        date = early_close_date.strftime("%#m/%#d/%Y")
        early_close_date_tz = from_timezone.localize(early_close_date)
        self.cme_group_futures_info[cme_class]["earlyCloses"][date] = early_close_date_tz.astimezone(
            pytz.timezone(cme_group_futures_info_timezone)).strftime("%H:%M:%S")

    def save_cme_group_futures_info(self):
        result = self.cme_group_futures_info.to_json()
        parsed = json.loads(result)
        with open("cme-group-futures-info.json", "w") as outfile:
            outfile.write(json.dumps(parsed, indent=2))

    def save(self):
        json_object = json.dumps(self.mhdb  , indent = 2)
        with open("market-hours-database-updated.json", "w") as outfile:
            outfile.write(json_object)

mhdb = market_hours_database()
changes = {
    "equity": {
        "earlyCloses": {
            datetime(2024, 1, 15, 12, 0, 0),
            datetime(2024, 2, 19, 12, 0, 0),
            datetime(2024, 5, 27, 12, 0, 0),
            datetime(2024, 6, 19, 12, 0, 0),
            datetime(2024, 7, 4, 12, 0, 0),
            datetime(2024, 9, 2, 12, 0, 0),
            datetime(2024, 11, 28, 12, 0, 0),
            datetime(2024, 12, 24, 12, 15, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 20, 12, 0, 0),
            datetime(2025, 2, 17, 12, 0, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 26, 12, 0, 0),
            datetime(2025, 6, 19, 12, 0, 0),
            datetime(2025, 7, 3, 12, 15, 0),
            datetime(2025, 7, 4, 12, 0, 0),
            datetime(2025, 9, 1, 12, 0, 0),
            datetime(2025, 11, 27, 12, 0, 0),
            datetime(2025, 11, 28, 12, 15, 0),
            datetime(2025, 12, 24, 12, 15, 0),
            datetime(2025, 12, 25, 16, 0, 0)
        },
        "lateOpens": {
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 11, 28, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 7, 3, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 11, 27, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays": {
            datetime(2025, 4, 18)
        }
    },
    "interest": {
        "earlyCloses": {
            datetime(2024, 1, 15, 12, 0, 0),
            datetime(2024, 2, 19, 12, 0, 0),
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 5, 27, 12, 0, 0),
            datetime(2024, 6, 19, 12, 0, 0),
            datetime(2024, 7, 4, 12, 0, 0),
            datetime(2024, 9, 2, 12, 0, 0),
            datetime(2024, 11, 28, 12, 0, 0),
            datetime(2024, 12, 24, 12, 15, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 20, 12, 0, 0),
            datetime(2025, 2, 17, 12, 0, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 26, 12, 0, 0),
            datetime(2025, 6, 19, 12, 0, 0),
            datetime(2025, 7, 4, 12, 0, 0),
            datetime(2025, 9, 1, 12, 0, 0),
            datetime(2025, 11, 27, 12, 0, 0),
            datetime(2025, 11, 28, 12, 15, 0),
            datetime(2025, 12, 24, 12, 15, 0),
            datetime(2025, 12, 25, 16, 0, 0)
        },
        "lateOpens": {
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 11, 28, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 11, 27, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays": {
            datetime(2025, 4, 18)
        }
    },
    "fx": {
        "earlyCloses": {
            datetime(2024, 1, 15, 16, 0, 0),
            datetime(2024, 2, 19, 16, 0, 0),
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 5, 27, 16, 0, 0),
            datetime(2024, 6, 19, 16, 0, 0),
            datetime(2024, 7, 4, 16, 0, 0),
            datetime(2024, 9, 2, 16, 0, 0),
            datetime(2024, 11, 28, 16, 0, 0),
            datetime(2024, 12, 24, 12, 15, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 20, 16, 0, 0),
            datetime(2025, 2, 17, 16, 0, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 26, 16, 0, 0),
            datetime(2025, 6, 19, 16, 0, 0),
            datetime(2025, 7, 4, 12, 0, 0),
            datetime(2025, 9, 1, 16, 0, 0),
            datetime(2025, 11, 27, 16, 0, 0),
            datetime(2025, 11, 28, 13, 45, 0),
            datetime(2025, 12, 24, 12, 15, 0),
        },
        "lateOpens": {
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 11, 28, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 11, 27, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays": {
            datetime(2025, 4, 18)
        }
    },
    "crypto": {
        "earlyCloses": {
            datetime(2024, 1, 15, 16, 0, 0),
            datetime(2024, 2, 19, 16, 0, 0),
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 5, 27, 16, 0, 0),
            datetime(2024, 6, 19, 16, 0, 0),
            datetime(2024, 7, 4, 16, 0, 0),
            datetime(2024, 9, 2, 16, 0, 0),
            datetime(2024, 11, 28, 16, 0, 0),
            datetime(2024, 12, 24, 12, 45, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 20, 16, 0, 0),
            datetime(2025, 2, 17, 16, 0, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 26, 16, 0, 0),
            datetime(2025, 6, 19, 16, 0, 0),
            datetime(2025, 7, 4, 12, 0, 0),
            datetime(2025, 9, 1, 16, 0, 0),
            datetime(2025, 11, 27, 16, 0, 0),
            datetime(2025, 11, 28, 13, 45, 0),
            datetime(2025, 12, 24, 12, 45, 0)
        },
        "lateOpens": {
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 11, 28, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 11, 27, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays": {
            datetime(2025, 4, 18)
        }
    },
    "energy": {
        "earlyCloses": {
            datetime(2024, 1, 15, 13, 30, 0),
            datetime(2024, 2, 19, 13, 30, 0),
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 5, 27, 13, 30, 0),
            datetime(2024, 6, 19, 13, 30, 0),
            datetime(2024, 7, 4, 13, 30, 0),
            datetime(2024, 9, 2, 13, 30, 0),
            datetime(2024, 11, 28, 13, 30, 0),
            datetime(2024, 12, 24, 12, 45, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 20, 13, 30, 0),
            datetime(2025, 2, 17, 13, 30, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 26, 13, 30, 0),
            datetime(2025, 6, 19, 13, 30, 0),
            datetime(2025, 7, 4, 12, 0, 0),
            datetime(2025, 9, 1, 13, 30, 0),
            datetime(2025, 11, 27, 13, 30, 0),
            datetime(2025, 11, 28, 13, 45, 0),
            datetime(2025, 12, 24, 12, 45, 0),
            datetime(2025, 12, 25, 16, 0, 0)
        },
        "lateOpens": {
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 11, 28, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 11, 27, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays": {
            datetime(2025, 4, 18)
        }
    },
    "metals": {
        "earlyCloses": {
            datetime(2024, 1, 15, 13, 30, 0),
            datetime(2024, 2, 19, 13, 30, 0),
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 5, 27, 13, 30, 0),
            datetime(2024, 6, 19, 13, 30, 0),
            datetime(2024, 7, 4, 13, 30, 0),
            datetime(2024, 9, 2, 13, 30, 0),
            datetime(2024, 11, 28, 13, 30, 0),
            datetime(2024, 12, 24, 12, 45, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 20, 13, 30, 0),
            datetime(2025, 2, 17, 13, 30, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 26, 13, 30, 0),
            datetime(2025, 6, 19, 13, 30, 0),
            datetime(2025, 7, 4, 12, 0, 0),
            datetime(2025, 9, 1, 13, 30, 0),
            datetime(2025, 11, 27, 13, 30, 0),
            datetime(2025, 11, 28, 13, 45, 0),
            datetime(2025, 12, 24, 12, 45, 0)
        },
        "lateOpens": {
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 11, 28, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 11, 27, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays": {
            datetime(2025, 4, 18)
        }
    },
    "grains": {
        "earlyCloses": {
            datetime(2023, 11, 22, 16, 0, 0),
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 12, 24, 12, 5, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 19, 16, 0, 0),
            datetime(2025, 2, 16, 16, 0, 0),
            datetime(2025, 4, 17, 16, 0, 0),
            datetime(2025, 5, 25, 16, 0, 0),
            datetime(2025, 6, 18, 16, 45, 0),
            datetime(2025, 7, 3, 16, 0, 0),
            datetime(2025, 8, 31, 16, 0, 0),
            datetime(2025, 11, 26, 16, 0, 0),
            datetime(2025, 11, 28, 12, 5, 0),
            datetime(2025, 12, 24, 12, 5, 0)
        },
        "lateOpens": {
            datetime(2024, 1, 15, 19, 0, 0),
            datetime(2024, 2, 19, 19, 0, 0),
            datetime(2024, 5, 27, 19, 0, 0),
            datetime(2024, 6, 19, 19, 0, 0),
            datetime(2024, 9, 2, 19, 0, 0),
            datetime(2025, 1, 20, 19, 0, 0),
            datetime(2025, 2, 17, 19, 0, 0),
            datetime(2025, 5, 26, 19, 0, 0),
            datetime(2025, 6, 19, 19, 0, 0),
            datetime(2025, 9, 1, 19, 0, 0),
        },
        "holidays":{
            datetime(2023, 11, 23),
            datetime(2023, 12, 25),
            datetime(2024, 1, 2),
            datetime(2024, 7, 4),
            datetime(2024, 11, 28),
            datetime(2024, 12, 25),
            datetime(2025, 1, 1),
            datetime(2025, 4, 18),
            datetime(2025, 7, 4),
            datetime(2025, 11, 27),
            datetime(2025, 12, 25)
        }
    },
    "dairy": {
        "earlyCloses": {
            datetime(2023, 11, 22, 16, 0, 0),
            datetime(2024, 1, 1, 17, 0, 0),
            datetime(2024, 3, 28, 13, 55, 0),
            datetime(2024, 11, 27, 16, 0, 0),
            datetime(2024, 12, 24, 12, 0, 0),
            datetime(2024, 12, 31, 16, 0, 0),
            datetime(2025, 1, 19, 16, 0, 0),
            datetime(2025, 2, 16, 16, 0, 0),
            datetime(2025, 4, 17, 13, 55, 0),
            datetime(2025, 5, 25, 16, 0, 0),
            datetime(2025, 7, 3, 12, 0, 0),
            datetime(2025, 8, 31, 16, 0, 0),
            datetime(2025, 11, 26, 16, 0, 0),
            datetime(2025, 12, 24, 12, 0, 0)
        },
        "lateOpens": {
            datetime(2023, 12, 25, 17, 0, 0),
            datetime(2024, 1, 15, 17, 0, 0),
            datetime(2024, 2, 19, 17, 0, 0),
            datetime(2024, 5, 27, 17, 0, 0),
            datetime(2024, 6, 19, 17, 0, 0),
            datetime(2024, 7, 4, 17, 0, 0),
            datetime(2024, 9, 2, 17, 0, 0),
            datetime(2024, 12, 25, 17, 0, 0),
            datetime(2025, 1, 1, 17, 0, 0),
            datetime(2025, 1, 20, 17, 0, 0),
            datetime(2025, 2, 17, 17, 0, 0),
            datetime(2025, 5, 26, 17, 0, 0),
            datetime(2025, 6, 19, 17, 0, 0),
            datetime(2025, 9, 1, 17, 0, 0),
            datetime(2025, 12, 25, 17, 0, 0)
        },
        "holidays":{
            datetime(2023, 11, 23),
            datetime(2024, 11, 28),
            datetime(2025, 4, 18),
            datetime(2025, 7, 4),
            datetime(2025, 11, 27),
            datetime(2025, 11, 28)
        }
    },
    "livestock": {
        "earlyCloses": {
            datetime(2024, 3, 28, 16, 0, 0),
            datetime(2024, 11, 27, 16, 0, 0),
            datetime(2025, 11, 28, 12, 5, 0),
            datetime(2025, 12, 24, 12, 15, 0)
        },
        "lateOpens": {
        },
        "holidays": {
            datetime(2023, 12, 25),
            datetime(2024, 1, 1),
            datetime(2024, 1, 15),
            datetime(2024, 2, 19),
            datetime(2024, 5, 27),
            datetime(2024, 6, 19),
            datetime(2024, 7, 4),
            datetime(2024, 9, 2),
            datetime(2024, 11, 28),
            datetime(2024, 12, 25),
            datetime(2025, 1, 1),
            datetime(2025, 1, 20),
            datetime(2025, 2, 17),
            datetime(2025, 4, 18),
            datetime(2025, 5, 26),
            datetime(2025, 6, 19),
            datetime(2025, 7, 4),
            datetime(2025, 9, 1),
            datetime(2025, 11, 27),
            datetime(2025, 12, 25)
        }
    },
    "lumber": {
        "earlyCloses": {
            datetime(2024, 3, 28, 15, 5, 0),
            datetime(2024, 11, 27, 15, 5, 0),
            datetime(2025, 11, 28, 12, 5, 0),
            datetime(2025, 12, 24, 12, 5, 0)
        },
        "lateOpens": {
        },
        "holidays": {
            datetime(2024, 1, 1),
            datetime(2024, 1, 15),
            datetime(2024, 2, 19),
            datetime(2024, 5, 27),
            datetime(2024, 6, 19),
            datetime(2024, 7, 4),
            datetime(2024, 9, 2),
            datetime(2024, 11, 28),
            datetime(2024, 12, 25),
            datetime(2025, 1, 1),
            datetime(2025, 1, 20),
            datetime(2025, 2, 17),
            datetime(2025, 4, 18),
            datetime(2025, 5, 26),
            datetime(2025, 6, 19),
            datetime(2025, 7, 4),
            datetime(2025, 9, 1),
            datetime(2025, 11, 27),
            datetime(2025, 12, 25)
        }
    }
}
"""
for cme_class in changes.keys():
    for early_close in changes[cme_class]["earlyCloses"]:
        #mhdb.add_early_close_to_mhdb(cme_class, early_close)
        mhdb.add_early_close_to_cme_group_futures_info(cme_class, early_close)
    for late_open in changes[cme_class]["lateOpens"]:
        #mhdb.add_late_open_to_mhdb(cme_class, late_open)
        mhdb.add_late_open_to_cme_group_futures_info(cme_class, late_open)

for cme_class in changes.keys():
    mhdb.update_early_closes(cme_class)
    mhdb.update_late_opens(cme_class)
    for holiday in changes[cme_class]["holidays"]:
        mhdb.add_holiday_to_mhdb(cme_class, holiday)"""

for cme_class in changes.keys():
    for early_close in changes[cme_class]["earlyCloses"]:
        mhdb.add_early_close_to_mhdb(cme_class, early_close)
        #mhdb.add_early_close_to_cme_group_futures_info(cme_class, early_close)
    for late_open in changes[cme_class]["lateOpens"]:
        mhdb.add_late_open_to_mhdb(cme_class, late_open)
        #mhdb.add_late_open_to_cme_group_futures_info(cme_class, late_open)
    for holiday in changes[cme_class]["holidays"]:
        mhdb.add_holiday_to_mhdb(cme_class, holiday)


print("NICE")
#mhdb.save_cme_group_futures_info()
mhdb.save()