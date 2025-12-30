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

    def add_late_open_to_mhdb(self, cme_class, late_open_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            
            timezone = self.mhdb["entries"][key]["exchangeTimeZone"]
            date = late_open_date.strftime("%#m/%#d/%Y")
            parsed_hour = late_open_date.astimezone(ZoneInfo(timezone)).strftime("%H:%M:%S")
            if "lateOpens" not in self.mhdb["entries"][key].keys():
                self.mhdb["entries"][key]["lateOpens"] = dict()
            if date not in self.mhdb["entries"][key]["lateOpens"].keys():
                print(f"Date {date} added it to {key} late opens")
                self.mhdb["entries"][key]["lateOpens"][date] = parsed_hour
                self.mhdb["entries"][key]["lateOpens"] = dict(sorted(self.mhdb["entries"][key]["lateOpens"].items(), key=lambda d: datetime.strptime(d[0], '%m/%d/%Y')))

    def add_early_close_to_mhdb(self, cme_class, early_close_date, is_update=False):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys():
                continue
            
            timezone = self.mhdb["entries"][key]["exchangeTimeZone"]
            date = early_close_date.strftime("%#m/%#d/%Y")
            parsed_hour = early_close_date.astimezone(ZoneInfo(timezone)).strftime("%H:%M:%S")
            if "earlyCloses" not in self.mhdb["entries"][key].keys():
                self.mhdb["entries"][key]["earlyCloses"] = dict()
            if date not in self.mhdb["entries"][key]["earlyCloses"].keys() or (is_update):
                print(f"Date {date} added it to {key} early closes")
                self.mhdb["entries"][key]["earlyCloses"][date] = parsed_hour
                self.mhdb["entries"][key]["earlyCloses"] = dict(sorted(self.mhdb["entries"][key]["earlyCloses"].items(), key=lambda d: datetime.strptime(d[0], '%m/%d/%Y')))

    def add_holiday_to_mhdb(self, cme_class, holiday_date):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            date = holiday_date.strftime("%#m/%#d/%Y")
            key = self.get_mhdb_key(product, products[product])
            if (key in self.mhdb["entries"].keys()) and (date not in self.mhdb["entries"][key]["holidays"]):
                print(f"Date {date} added it to {key} holidays")
                self.mhdb["entries"][key]["holidays"].append(date)
                self.mhdb["entries"][key]["holidays"] = sorted(self.mhdb["entries"][key]["holidays"], key=lambda d: datetime.strptime(d, '%m/%d/%Y'))

    def add_bank_holiday_to_mhdb(self, cme_class, holiday_date, exclude):
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            date = holiday_date.strftime("%#m/%#d/%Y")
            if product in exclude: continue
            key = self.get_mhdb_key(product, products[product])
            
            if (key in self.mhdb["entries"].keys()) and ("bankHolidays" not in self.mhdb["entries"][key].keys()):
                self.mhdb["entries"][key]["bankHolidays"] = list()
            if (key in self.mhdb["entries"].keys()) and (date not in self.mhdb["entries"][key]["bankHolidays"]):
                print(f"Date {date} added it to {key} bank holidays")
                self.mhdb["entries"][key]["bankHolidays"].append(date)
                self.mhdb["entries"][key]["bankHolidays"] = sorted(self.mhdb["entries"][key]["bankHolidays"], key=lambda d: datetime.strptime(d, '%m/%d/%Y'))
    
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

    def add_early_closes(self, cme_class, early_closes):
        for early_close in early_closes:
            self.add_early_close_to_mhdb(cme_class, early_close)
            #mhdb.add_early_close_to_cme_group_futures_info(cme_class, early_close)

    def add_late_opens(self, cme_class, late_opens):
        for late_open in late_opens:
            self.add_late_open_to_mhdb(cme_class, late_open)
            #mhdb.add_late_open_to_cme_group_futures_info(cme_class, late_open)
    
    def add_holidays(self, cme_class, holidays):
        for holiday in holidays:
            self.add_holiday_to_mhdb(cme_class, holiday)

    def add_bank_holidays(self, cme_class, bank_holidays, exclude):
        for holiday in bank_holidays:
            self.add_bank_holiday_to_mhdb(cme_class, holiday, exclude)

    def add_all(self, cme_class, changes, exclude=[]):
        self.add_early_closes(cme_class, changes[cme_class]["earlyCloses"])
        self.add_late_opens(cme_class, changes[cme_class]["lateOpens"])
        self.add_holidays(cme_class, changes[cme_class]["holidays"])
        self.add_bank_holidays(cme_class, changes[cme_class]["bankHolidays"], exclude)
    
    def remove_early_closes(self, cme_class, early_closes):
        for early_close in early_closes:
            self.remove_early_close_from_mhdb(cme_class, early_close)

    def remove_late_opens(self, cme_class, late_opens):
        for late_open in late_opens:
            self.remove_late_open_from_mhdb(cme_class, late_open)
    
    def remove_holidays(self, cme_class, holidays):
        for holiday in holidays:
            self.remove_holiday_from_mhdb(cme_class, holiday)

    def remove_bank_holidays(self, cme_class, bank_holidays):
        for bank_holiday in bank_holidays:
            self.remove_bank_holiday_from_mhdb(cme_class, bank_holiday)

    def remove_all(self, cme_class, changes):
        self.remove_early_closes(cme_class, changes[cme_class]["remove"]["earlyCloses"])
        self.remove_late_opens(cme_class, changes[cme_class]["remove"]["lateOpens"])
        self.remove_holidays(cme_class, changes[cme_class]["remove"]["holidays"])
        self.remove_bank_holidays(cme_class, changes[cme_class]["remove"]["bankHolidays"])

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
        for cme_class in changes.keys():
            timezone = changes[cme_class]["exchangeTimeZone"]
            changes_df[cme_class] = {}
            
            changes_df[cme_class]["earlyCloses"] = self.parse_dictionary_of_dates(timezone, changes[cme_class]["earlyCloses"])
            changes_df[cme_class]["lateOpens"] = self.parse_dictionary_of_dates(timezone, changes[cme_class]["lateOpens"])
            changes_df[cme_class]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes[cme_class]["holidays"]]
            changes_df[cme_class]["bankHolidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes[cme_class]["bankHolidays"]]

            changes_df[cme_class]["remove"] = {}
            changes_df[cme_class]["remove"]["earlyCloses"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes[cme_class]["remove"]["earlyCloses"]]
            changes_df[cme_class]["remove"]["lateOpens"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes[cme_class]["remove"]["lateOpens"]]
            changes_df[cme_class]["remove"]["holidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes[cme_class]["remove"]["holidays"]]
            changes_df[cme_class]["remove"]["bankHolidays"] = [datetime.strptime(f"{date}", "%m/%d/%Y") for date in changes[cme_class]["remove"]["bankHolidays"]]
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
        print(f"Checking duplicates in {cme_class} entries...")
        entry = self.cme_group_futures_info[cme_class]
        products = entry["cmeKeys"]
        for product in products.keys():
            key = self.get_mhdb_key(product, products[product])
            if key not in self.mhdb["entries"].keys(): continue

            holidays = [date for date in self.mhdb["entries"][key]["holidays"]]
            duplicates = {
                date: count
                for date, count in Counter(holidays).items()
                if count > 1
            }
            if len(duplicates) != 0: print(f"Duplicated {key} holidays: {duplicates}\n")

            bank_holidays = [date for date in self.mhdb["entries"][key]["bankHolidays"]]
            duplicates = {
                date: count
                for date, count in Counter(bank_holidays).items()
                if count > 1
            }
            if len(duplicates) != 0: print(f"Duplicated {key} bank holidays: {duplicates}\n")

            early_closes = [date for date in self.mhdb["entries"][key]["earlyCloses"]]
            duplicates = {
                date: count
                for date, count in Counter(early_closes).items()
                if count > 1
            }
            if len(duplicates) != 0: print(f"Duplicated {key} early closes: {duplicates}\n")

            late_opens = [date for date in self.mhdb["entries"][key]["lateOpens"]]
            duplicates = {
                date: count
                for date, count in Counter(late_opens).items()
                if count > 1
            }
            if len(duplicates) != 0: print(f"Duplicated {key} late opens: {duplicates}\n")
    
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
mhdb.add_all("equity", changes)
mhdb.add_all("interest", changes)
mhdb.add_all("fx", changes, ["MNH", "CNH", "MIR"]) # Remember to exclude MNH, CNH and MIR keys
mhdb.add_all("crypto", changes)
mhdb.add_all("energy", changes)
mhdb.add_all("metals", changes)
mhdb.add_all("grains", changes) # Remember to include oilseeds
mhdb.add_all("dairy", changes)
mhdb.add_all("livestock", changes)
mhdb.add_all("lumber", changes)
mhdb.add_all("softs", changes)

print("Check changes...")
# Check changes
mhdb.check_duplicates("equity")
mhdb.check_duplicates("interest")
mhdb.check_duplicates("fx")
mhdb.check_duplicates("crypto")
mhdb.check_duplicates("energy")
mhdb.check_duplicates("metals")
mhdb.check_duplicates("grains")
mhdb.check_duplicates("dairy")
mhdb.check_duplicates("livestock")
mhdb.check_duplicates("lumber")
mhdb.check_duplicates("softs")

#print("Find new entries")
#mhdb.find_new_entries()

print("Remove dates")
mhdb.remove_all("equity", changes)
mhdb.remove_all("interest", changes)
mhdb.remove_all("fx", changes) # Remember to exclude MNH, CNH and MIR keys
mhdb.remove_all("crypto", changes)
mhdb.remove_all("energy", changes)
mhdb.remove_all("metals", changes)
mhdb.remove_all("grains", changes) # Remember to include oilseeds
mhdb.remove_all("dairy", changes)
mhdb.remove_all("livestock", changes)
mhdb.remove_all("lumber", changes)
mhdb.remove_all("softs", changes)
mhdb.save()