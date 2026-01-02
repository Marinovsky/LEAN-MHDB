from time import timezone
import pytz

import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import OrderedDict
from collections import Counter
import requests
import json

class market_hours_database:
    def __init__(self):
        self.mhdb = self.get_mhdb_entries_from_local()
        self.cme_group_futures_info = self.get_cme_group_future_info_from_local()
        self.ice_futures_info = self.get_ice_future_info_from_cloud()
        self.cme_equities_filename = "cme_equities.xlsx"
        self.cme_interest_rate_filename = "cme_interest_rate.xlsx"
        self.cme_fx_filename = "cme_fx.xlsx"
        self.cme_crypto_filename = "cme_crypto.xlsx"
        self.cme_energy_filename = "cme_energy.xlsx"
        self.cme_metals_filename = "cme_metals.xlsx"
        self.cme_grains_filename = "cme_grains.xlsx"
        self.cme_lumber_filename = "cme_lumber.xlsx"
        self.cme_livestock_filename = "cme_livestock.xlsx"
        self.cme_dairy_filename = "cme_dairy.xlsx"

    def get_mhdb_entries(self):
        url = "https://raw.githubusercontent.com/Marinovsky/Lean/master/Data/market-hours/market-hours-database.json"
        file = requests.get(url)
        data = json.loads(file.content, object_pairs_hook=OrderedDict)
        return data

    def get_mhdb_entries_from_local(self):
        with open("market-hours-database.json", "r") as f:
            data = json.load(f)
        return data

    def get_cme_group_future_info_from_cloud(self):
        url = "https://www.dropbox.com/scl/fi/09j95smm8ko09aupx66gl/cme-group-futures-info.json?rlkey=43ne4e093vtsqo6o2vnbrvtfh&st=7honpni6&dl=1"
        df = pd.read_json(url)
        return df

    def get_ice_future_info_from_cloud(self):
        url = "https://www.dropbox.com/scl/fi/hbgp1j70jn2c0zsiy7vuf/ice-futures-info.json?rlkey=6yhs8a5nw16urdmyuhtnlb0lv&st=6xbh4im7&dl=1"
        df = pd.read_json(url)
        return df

    def get_cme_group_future_info_from_local(self):
        df = {}
        df["equity"] = {}
        df["interest"] = {}
        df["fx"] = {}
        df["crypto"] = {}
        df["energy"] = {}
        df["metals"] = {}
        df["grains"] = {}
        df["dairy"] = {}
        df["livestock"] = {}
        df["lumber"] = {}
        df["softs"] = {}

        df["lumber"]["cmeKeys"] = {"LBR": "cme"}
        df["softs"]["cmeKeys"] = {"CJ": "nymex", "KT": "nymex", "YO": "nymex", "TT": "nymex"}

        df["equity"]["cmeKeys"] = self.get_cme_equities_keys()
        df["interest"]["cmeKeys"] = self.get_cme_interest_rate_keys()
        df["fx"]["cmeKeys"] = self.get_cme_fx_keys()
        df["crypto"]["cmeKeys"] = self.get_cme_crypto_keys()
        df["energy"]["cmeKeys"] = self.get_cme_energy_keys()
        df["metals"]["cmeKeys"] = self.get_cme_metals_keys()
        df["grains"]["cmeKeys"] = self.get_cme_grains_keys()
        df["dairy"]["cmeKeys"] = self.get_cme_dairy_keys()
        df["livestock"]["cmeKeys"] = self.get_cme_livestock_keys()

        return df

    def get_cme_equities_keys(self):
        return self._get_cme_keys("cme_equities.xlsx")

    def get_cme_interest_rate_keys(self):
        return self._get_cme_keys("cme_interest_rate.xlsx")

    def get_cme_fx_keys(self):
        return self._get_cme_keys("cme_fx.xlsx")

    def get_cme_crypto_keys(self):
        return self._get_cme_keys("cme_crypto.xlsx")

    def get_cme_energy_keys(self):
        return self._get_cme_keys("cme_energy.xlsx")

    def get_cme_metals_keys(self):
        return self._get_cme_keys("cme_metals.xlsx")

    def get_cme_grains_keys(self):
        return self._get_cme_keys("cme_grains.xlsx")

    def get_cme_lumber_keys(self):
        return self._get_cme_keys("cme_lumber.xlsx")

    def get_cme_livestock_keys(self):
        return self._get_cme_keys("cme_livestock.xlsx")

    def get_cme_dairy_keys(self):
        return self._get_cme_keys("cme_dairy.xlsx")

    def _get_cme_keys(self, filename):
        df = pd.read_excel(filename)
        merge_columns_function = lambda row: f"{str(row['Unnamed: 2'])}:{str(row['Unnamed: 5']).lower()}"
        symbols_and_markets_df = df.apply(merge_columns_function, axis=1)
        symbols_and_markets_list = [entry for entry in symbols_and_markets_df if ("nan" not in entry and "Globex" not in entry)]
        symbols_and_markets_list = list(set(symbols_and_markets_list))
        symbols_keys1 = {}
        for item in symbols_and_markets_list:
            symbol = item.split(":")[0]
            market = item.split(":")[1]
            symbols_keys1[symbol] = market
        
        merge_columns_function = lambda row: f"{str(row['Unnamed: 1'])}:{str(row['Unnamed: 5']).lower()}"
        symbols_and_markets_df = df.apply(merge_columns_function, axis=1)
        symbols_and_markets_list = [entry for entry in symbols_and_markets_df if ("nan" not in entry and "Globex" not in entry)]
        symbols_and_markets_list = list(set(symbols_and_markets_list))
        symbols_keys2 = {}
        for item in symbols_and_markets_list:
            symbol = item.split(":")[0]
            market = item.split(":")[1]
            symbols_keys2[symbol] = market
        
        return symbols_keys1 | symbols_keys2

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

    def add_date_to_dict(self, key, label, date_to_add):
        timezone = self.mhdb["entries"][key]["exchangeTimeZone"]
        date = date_to_add.strftime("%#m/%#d/%Y")
        parsed_hour = date_to_add.astimezone(ZoneInfo(timezone)).strftime("%H:%M:%S")
        if label not in self.mhdb["entries"][key].keys():
            self.mhdb["entries"][key][label] = dict()
        if date not in self.mhdb["entries"][key][label].keys():
            print(f"Date {date} added it to {key} {label}")
            self.mhdb["entries"][key][label][date] = parsed_hour
            self.mhdb["entries"][key][label] = dict(sorted(self.mhdb["entries"][key][label].items(), key=lambda d: datetime.strptime(d[0], '%m/%d/%Y')))

    def remove_date_to_dict(self, key, label, date_to_remove):
        date = date_to_remove.strftime("%#m/%#d/%Y")
        if label not in self.mhdb["entries"][key].keys():
            print(f"{label} not present in {key} entry")
        if date in self.mhdb["entries"][key][label].keys():
            print(f"Date {date} removed from {key} {label}")
            self.mhdb["entries"][key][label].pop(date, None)
    
    def add_date_to_list(self, key, label, date_to_add):
        date = date_to_add.strftime("%#m/%#d/%Y")
        if (key in self.mhdb["entries"].keys()) and (label not in self.mhdb["entries"][key].keys()):
            self.mhdb["entries"][key][label] = list()
        if (key in self.mhdb["entries"].keys()) and (date not in self.mhdb["entries"][key][label]):
            print(f"Date {date} added it to {key} {label}")
            self.mhdb["entries"][key][label].append(date)
            self.mhdb["entries"][key][label] = sorted(self.mhdb["entries"][key][label], key=lambda d: datetime.strptime(d, '%m/%d/%Y'))

    def remove_date_from_list(self, key, label, date_to_remove):
        date = date_to_remove.strftime("%#m/%#d/%Y")
        if (key in self.mhdb["entries"].keys()) and (label not in self.mhdb["entries"][key].keys()):
            print(f"{label} not present in {key} entry")
        if (key in self.mhdb["entries"].keys()) and (date in self.mhdb["entries"][key][label]):
            print(f"Date {date} removed from {key} {label}")
            self.mhdb["entries"][key][label] = [e for e in self.mhdb["entries"][key][label] if e != date]

    def add_cme_late_open_to_mhdb(self, cme_class, late_open_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            self.add_date_to_dict(key, "lateOpens", late_open_date)

    def add_cme_early_close_to_mhdb(self, cme_class, early_close_date, is_update=False):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            
            self.add_date_to_dict(key, "earlyCloses", early_close_date)

    def add_cme_holiday_to_mhdb(self, cme_class, holiday_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            self.add_date_to_list(key, "holidays", holiday_date)
    
    def add_ice_holiday_to_mhdb(self, ice_class, holiday):
        entry = self.ice_futures_info[ice_class]
        for product in entry["keys"]:
            key = self.get_mhdb_key(product, "ice")
            if key not in self.mhdb["entries"].keys():
                continue
            self.add_date_to_list(key, "holidays", holiday)

    def add_cme_bank_holiday_to_mhdb(self, cme_class, holiday_date, exclude):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            if product in exclude: continue
            key = self.get_mhdb_key(product, products[product])
            self.add_date_to_list(key, "bankHolidays", holiday_date)
    
    def remove_early_close_from_mhdb(self, cme_class, early_close_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            
            date = early_close_date.strftime("%#m/%#d/%Y")
            if "earlyCloses" not in self.mhdb["entries"][key].keys():
                continue
            self.mhdb["entries"][key]["earlyCloses"].pop(date, None)

    def remove_late_open_from_mhdb(self, cme_class, late_open_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            
            date = late_open_date.strftime("%#m/%#d/%Y")
            if "lateOpens" not in self.mhdb["entries"][key].keys():
                continue
            print(f"Date {date} removed from {key} late opens")
            self.mhdb["entries"][key]["lateOpens"].pop(date, None)
    
    def remove_holiday_from_mhdb(self, cme_class, holiday_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            date = holiday_date.strftime("%#m/%#d/%Y")
            key = self.get_mhdb_key(product, products[product])
            
            if (key in self.mhdb["entries"].keys()) and ("holidays" not in self.mhdb["entries"][key].keys()):
                continue
            if (key in self.mhdb["entries"].keys()) and (date in self.mhdb["entries"][key]["holidays"]):
                print(f"Date {date} removed from {key} holidays")
                self.mhdb["entries"][key]["holidays"].remove(date)
                self.mhdb["entries"][key]["holidays"] = sorted(self.mhdb["entries"][key]["holidays"], key=lambda d: datetime.strptime(d, '%m/%d/%Y'))
    
    def remove_bank_holiday_from_mhdb(self, cme_class, holiday_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            date = holiday_date.strftime("%#m/%#d/%Y")
            key = self.get_mhdb_key(product, products[product])
            
            if (key in self.mhdb["entries"].keys()) and ("bankHolidays" not in self.mhdb["entries"][key].keys()):
                continue
            if (key in self.mhdb["entries"].keys()) and (date in self.mhdb["entries"][key]["bankHolidays"]):
                print(f"Date {date} removed from {key} bank holidays")
                self.mhdb["entries"][key]["bankHolidays"].remove(date)
                self.mhdb["entries"][key]["bankHolidays"] = sorted(self.mhdb["entries"][key]["bankHolidays"], key=lambda d: datetime.strptime(d, '%m/%d/%Y'))

    def add_bank_holidays_entry_to_mhdb(self):
        for entry in self.mhdb["entries"].keys():
            if "bankHolidays" not in self.mhdb["entries"][entry].keys():
                self.mhdb["entries"][entry]["bankHolidays"] = []

    def add_late_open_to_cme_group_futures_info(self, cme_class, late_open_date):
        entry = self.cme_group_futures_info[cme_class]
        cme_group_futures_info_timezone = entry["exchangeTimeZone"]
        date = late_open_date.strftime("%#m/%#d/%Y")
        self.cme_group_futures_info[cme_class]["lateOpens"][date] = late_open_date.astimezone(
            ZoneInfo(cme_group_futures_info_timezone)).strftime("%H:%M:%S")

    def add_early_close_to_cme_group_futures_info(self, cme_class, early_close_date):
        entry = self.cme_group_futures_info[cme_class]
        cme_group_futures_info_timezone = entry["exchangeTimeZone"]
        date = early_close_date.strftime("%#m/%#d/%Y")
        self.cme_group_futures_info[cme_class]["earlyCloses"][date] = early_close_date.astimezone(
            ZoneInfo(cme_group_futures_info_timezone)).strftime("%H:%M:%S")

    def add_cme_early_closes(self, cme_class, early_closes):
        for early_close in early_closes:
            self.add_cme_early_close_to_mhdb(cme_class, early_close)
            #mhdb.add_early_close_to_cme_group_futures_info(cme_class, early_close)

    def add_early_closes(self, key, early_closes):
        for early_close in early_closes:
            self.add_date_to_dict(key, "earlyCloses", early_close)

    def add_late_opens(self, key, late_opens):
        for late_open in late_opens:
            self.add_date_to_dict(key, "lateOpens", late_open)

    def add_holidays(self, key, holidays):
        for holiday in holidays:
            self.add_date_to_list(key, "holidays", holiday)
    
    def remove_holidays(self, key, holidays):
        for holiday in holidays:
            self.remove_date_from_list(key, "holidays", holiday)

    def add_cme_late_opens(self, cme_class, late_opens):
        for late_open in late_opens:
            self.add_cme_late_open_to_mhdb(cme_class, late_open)
            #mhdb.add_late_open_to_cme_group_futures_info(cme_class, late_open)
    
    def add_cme_holidays(self, cme_class, holidays):
        for holiday in holidays:
            self.add_cme_holiday_to_mhdb(cme_class, holiday)

    def add_ice_holidays(self, cme_class, holidays):
        for holiday in holidays:
            self.add_ice_holiday_to_mhdb(cme_class, holiday)

    def add_cme_bank_holidays(self, cme_class, bank_holidays, exclude):
        for holiday in bank_holidays:
            self.add_cme_bank_holiday_to_mhdb(cme_class, holiday, exclude)

    def apply_cme_changes(self, cme_class, changes, exclude=[]):
        self.add_cme_early_closes(cme_class, changes["cme"][cme_class]["earlyCloses"])
        self.add_cme_late_opens(cme_class, changes["cme"][cme_class]["lateOpens"])
        self.add_cme_holidays(cme_class, changes["cme"][cme_class]["holidays"])
        self.add_cme_bank_holidays(cme_class, changes["cme"][cme_class]["bankHolidays"], exclude)

    def apply_ice_changes(self, cme_class, changes):
        self.add_ice_holidays(cme_class, changes["ice"][cme_class]["holidays"])
    
    def remove_cme_early_closes(self, cme_class, early_closes):
        for early_close in early_closes:
            self.remove_early_close_from_mhdb(cme_class, early_close)

    def remove_cme_late_opens(self, cme_class, late_opens):
        for late_open in late_opens:
            self.remove_late_open_from_mhdb(cme_class, late_open)
    
    def remove_cme_holidays(self, cme_class, holidays):
        for holiday in holidays:
            self.remove_holiday_from_mhdb(cme_class, holiday)

    def remove_cme_bank_holidays(self, cme_class, bank_holidays):
        for bank_holiday in bank_holidays:
            self.remove_bank_holiday_from_mhdb(cme_class, bank_holiday)

    def remove_all(self, cme_class, changes):
        self.remove_cme_early_closes(cme_class, changes["cme"][cme_class]["remove"]["earlyCloses"])
        self.remove_cme_late_opens(cme_class, changes["cme"][cme_class]["remove"]["lateOpens"])
        self.remove_cme_holidays(cme_class, changes["cme"][cme_class]["remove"]["holidays"])
        self.remove_cme_bank_holidays(cme_class, changes["cme"][cme_class]["remove"]["bankHolidays"])

    def parse_dictionary_of_dates(self, timezone, dates):
        dates_parsed = []
        for date in dates.keys():
            parsed = datetime.strptime(f"{date} {dates[date]}", "%m/%d/%Y %H:%M:%S")
            parsed = parsed.replace(tzinfo=ZoneInfo(timezone))
            dates_parsed.append(parsed)
        return dates_parsed
    
    def parse_list_of_dates(self, timezone, dates):
        dates_parsed = []
        for date in dates:
            parsed = datetime.strptime(f"{date}", "%m/%d/%Y")
            parsed = parsed.replace(tzinfo=ZoneInfo(timezone))
            dates_parsed.append(parsed)
        return dates_parsed

    def read_changes_from_json(self, path):
        with open(path, "r") as f:
            changes = json.load(f)
        changes_df = {}
        changes_df["cme"] = {}
        for cme_class in changes["cme"].keys():
            timezone = changes["cme"][cme_class]["exchangeTimeZone"]
            changes_df["cme"][cme_class] = {}
            
            changes_df["cme"][cme_class]["earlyCloses"] = self.parse_dictionary_of_dates(timezone, changes["cme"][cme_class]["earlyCloses"])
            changes_df["cme"][cme_class]["lateOpens"] = self.parse_dictionary_of_dates(timezone, changes["cme"][cme_class]["lateOpens"])
            changes_df["cme"][cme_class]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cme"][cme_class]["holidays"]]
            changes_df["cme"][cme_class]["bankHolidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cme"][cme_class]["bankHolidays"]]

            changes_df["cme"][cme_class]["remove"] = {}
            changes_df["cme"][cme_class]["remove"]["earlyCloses"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cme"][cme_class]["remove"]["earlyCloses"]]
            changes_df["cme"][cme_class]["remove"]["lateOpens"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cme"][cme_class]["remove"]["lateOpens"]]
            changes_df["cme"][cme_class]["remove"]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cme"][cme_class]["remove"]["holidays"]]
            changes_df["cme"][cme_class]["remove"]["bankHolidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cme"][cme_class]["remove"]["bankHolidays"]]
        
        changes_df["eurex"] = {}
        changes_df["eurex"]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["eurex"]["holidays"]]

        changes_df["ice"] = {}
        for ice_class in changes["ice"].keys():
            changes_df["ice"][ice_class] = {}
            changes_df["ice"][ice_class]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["ice"][ice_class]["holidays"]]

        changes_df["cfe"] = {}
        changes_df["cfe"]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["cfe"]["holidays"]]
        changes_df["cfe"]["earlyCloses"] = self.parse_dictionary_of_dates(timezone, changes["cfe"]["earlyCloses"])

        changes_df["oanda"] = {}
        changes_df["oanda"]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["oanda"]["holidays"]]
        changes_df["oanda"]["lateOpens"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["oanda"]["lateOpens"]]

        changes_df["oanda"]["remove"] = {}
        changes_df["oanda"]["remove"]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes["oanda"]["remove"]["holidays"]]
        
        return changes_df

    def save_cme_group_futures_info(self):
        result = self.cme_group_futures_info.to_json()
        parsed = json.loads(result)
        with open("cme-group-futures-info.json", "w") as outfile:
            outfile.write(json.dumps(parsed, indent=2))

    def save(self):
        json_object = json.dumps(self.mhdb  , indent = 2)
        with open("market-hours-database-updated.json", "w") as outfile:
            outfile.write(json_object)
    
    def check_duplicates(self, cme_class):
        print(f"Checking duplicates in mhdb")
        for key in self.mhdb["entries"].keys():
            holidays = [date for date in self.mhdb["entries"][key]["holidays"]]
            duplicates = {
                date: count
                for date, count in Counter(holidays).items()
                if count > 1
            }
            if len(duplicates) != 0:
                print(f"Duplicated {key} holidays: {duplicates}\n")
                for date in duplicates:
                    date_formatted = datetime.strptime(f"{date}", "%m/%d/%Y")
                    self.remove_date_from_list(key, "holidays", date_formatted)

            if "bankHolidays" in self.mhdb["entries"][key].keys():
                bank_holidays = [date for date in self.mhdb["entries"][key]["bankHolidays"]]
                duplicates = {
                    date: count
                    for date, count in Counter(bank_holidays).items()
                    if count > 1
                }
                if len(duplicates) != 0:
                    print(f"Duplicated {key} bank holidays: {duplicates}\n")
                    for date in duplicates:
                        date_formatted = datetime.strptime(f"{date}", "%m/%d/%Y")
                        self.remove_date_from_list(key, "bankHolidays", date_formatted)
            
            if "earlyCloses" in self.mhdb["entries"][key].keys():
                early_closes = [date for date in self.mhdb["entries"][key]["earlyCloses"]]
                duplicates = {
                    date: count
                    for date, count in Counter(early_closes).items()
                    if count > 1
                }
                if len(duplicates) != 0:
                    print(f"Duplicated {key} early closes: {duplicates}\n")
                    for date in duplicates:
                        date_formatted = datetime.strptime(f"{date}", "%m/%d/%Y")
                        self.remove_date_from_dict(key, "earlyCloses", date_formatted)
            
            if "lateOpens" in self.mhdb["entries"][key].keys():
                late_opens = [date for date in self.mhdb["entries"][key]["lateOpens"]]
                duplicates = {
                    date: count
                    for date, count in Counter(late_opens).items()
                    if count > 1
                }
                if len(duplicates) != 0:
                    print(f"Duplicated {key} late opens: {duplicates}\n")
                    for date in duplicates:
                        date_formatted = datetime.strptime(f"{date}", "%m/%d/%Y")
                        self.remove_date_from_dict(key, "lateOpens", date_formatted)
    
    def find_new_entries(self):
        for entry in self.mhdb["entries"]:
            cme_code = entry.split("-")[2]
            market = entry.split("-")[1]
            if cme_code == "[*]": continue
            if market not in ["cme", "cbot", "nymex", "comex"]: continue

            is_old_entry = True
            for cme_class in self.cme_group_futures_info.keys():
                if cme_code in self.cme_group_futures_info[cme_class]["cmeKeys"].keys() and self.cme_group_futures_info[cme_class]["cmeKeys"][cme_code] == market:
                    is_old_entry = False
            
            if is_old_entry:
                print(f"Product {cme_code} is a new entry")
    
    def check_intersection_of_holidays_and_label(self, entry, label):
        holidays = [date for date in self.mhdb["entries"][entry]["holidays"]]
        if label not in self.mhdb["entries"][entry].keys(): return

        try:
            dates = [date for date in self.mhdb["entries"][entry][label].keys()]
        except:
            dates = [date for date in self.mhdb["entries"][entry][label]]
        intersection = list(set(holidays) & set(dates))
        if len(intersection) != 0:
            print(f"The following dates belong to both holidays and {label} of {entry}: {intersection}")

    def check_disjoint_holidays(self):
        for entry in self.mhdb["entries"]:
            self.check_intersection_of_holidays_and_label(entry, "earlyCloses")
            self.check_intersection_of_holidays_and_label(entry, "lateOpens")
            self.check_intersection_of_holidays_and_label(entry, "bankHolidays")
    
    def check_disjoint_holidays_with_parent(self):
        for entry in self.mhdb["entries"]:
            product = entry.split("-")
            parent = f"{product[0]}-{product[1]}-[*]"
            if (parent != entry) and (parent in self.mhdb["entries"].keys()):
                product_holidays = [date for date in self.mhdb["entries"][entry]["holidays"]]
                parent_holidays = [date for date in self.mhdb["entries"][parent]["holidays"]]

                intersection = list(set(product_holidays) & set(parent_holidays))
                if len(intersection) != 0:
                    print(f"The following dates belong to both holidays of {entry} and its genric entry {parent}: {intersection}")
                    print(f"Dates removed from {entry} holidays")
                    self.mhdb["entries"][entry]["holidays"] = [date for date in self.mhdb["entries"][entry]["holidays"] if date not in intersection]

                if "earlyCloses" in self.mhdb["entries"][entry].keys() and  "earlyCloses" in self.mhdb["entries"][parent].keys():
                    product_early_closes = [date for date in self.mhdb["entries"][entry]["earlyCloses"].keys()]
                    parent_early_closes = [date for date in self.mhdb["entries"][parent]["earlyCloses"].keys()]

                    intersection = list(set(product_early_closes) & set(parent_early_closes))
                    if len(intersection) != 0:
                        print(f"The following dates belong to both early closes of {entry} and its genric entry {parent}: {intersection}")
                        print(f"Dates removed from {entry} early Closes")
                        for date in intersection:
                            self.mhdb["entries"][entry]["earlyCloses"].pop(date, None)
                
                if "lateOpens" in self.mhdb["entries"][entry].keys() and  "lateOpens" in self.mhdb["entries"][parent].keys():
                    product_late_opens = [date for date in self.mhdb["entries"][entry]["lateOpens"].keys()]
                    parent_late_opens = [date for date in self.mhdb["entries"][parent]["lateOpens"].keys()]

                    intersection = list(set(product_late_opens) & set(parent_late_opens))
                    if len(intersection) != 0:
                        print(f"The following dates belong to both late opens of {entry} and its genric entry {parent}: {intersection}")
                        print(f"Dates removed from {entry} late opens")
                        for date in intersection:
                            self.mhdb["entries"][entry]["lateOpens"].pop(date, None)
                
                if "bankHolidays" in self.mhdb["entries"][entry].keys() and  "bankHolidays" in self.mhdb["entries"][parent].keys():
                    product_bank_holidays = [date for date in self.mhdb["entries"][entry]["bankHolidays"]]
                    parent_bank_holidays = [date for date in self.mhdb["entries"][parent]["bankHolidays"]]

                    intersection = list(set(product_bank_holidays) & set(parent_bank_holidays))
                    if len(intersection) != 0:
                        print(f"The following dates belong to both bank holidays of {entry} and its genric entry {parent}: {intersection}")
                        print(f"Dates removed from {entry} bank holidays")
                        self.mhdb["entries"][entry]["holidays"] = [date for date in self.mhdb["entries"][entry]["holidays"] if date not in intersection]
                print("\n")

mhdb = market_hours_database()
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

print("NICE")
#mhdb.add_bank_holidays_entry_to_mhdb()
#mhdb.save_cme_group_futures_info()

changes = mhdb.read_changes_from_json("changes.json")
mhdb.check_disjoint_holidays_with_parent()
mhdb.check_disjoint_holidays()
mhdb.check_duplicates("")
mhdb.remove_all("dairy", changes)
mhdb.remove_all("livestock", changes)
mhdb.remove_all("lumber", changes)
mhdb.remove_holidays("Forex-oanda-[*]", changes["oanda"]["remove"]["holidays"])
mhdb.remove_date_from_list("Future-cbot-KE", "holidays", datetime(2023, 9, 4))
mhdb.save()