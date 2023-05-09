import pandas as pd
import json
import os
import time
from django.conf import settings
import requests
from datetime import datetime
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_API_TOKEN = "your_api_token"


class MondayDeals():
    def __init__(self):
        self.headers = {"Authorization": MONDAY_API_TOKEN}
        self.general_email_company_list = ["info", "office", "service", "support", "kundenservice", "kontakt", "website", "contact", "kundenbetreuung", "welcome"]
        self.title_ceo_list = ["ceo", "geschäftsführer", "geschäftsführerin", "inhaber", "inhaberin", "vorstand", "vorsitzender", "vorstandsvorsitzender"]
        self.invalid_rows_df = pd.DataFrame()
        self.users_id_dict = {}
        self.statuses_dict = {
            "item_status": "",
            "status_1": "",
            "status_2": ""
        }

    def send_api_request(self, data_dict):
        result = None
        for x in range(1, 5):
            try:
                time.sleep(2)
                response = requests.request('POST', MONDAY_API_URL, headers=self.headers, data=data_dict)
                result = response.json()
                break
            except Exception as error:
                time.sleep(10)
        return result

    def update_status_dict(self, file_status, file_status_mapping_dict):
        self.statuses_dict["item_status"] = file_status_mapping_dict[file_status]["item_status"]
        self.statuses_dict["status_1"] = file_status_mapping_dict[file_status]["status_1"]
        self.statuses_dict["status_2"] = file_status_mapping_dict[file_status]["status_2"]


    def get_board_status(self, df):
        self.statuses_dict = {
            "item_status": "",
            "status_1": "",
            "status_2": ""
        }
        file_status_mapping_dict = {
            "Attempted to contact": {
                "item_status": "Attempted to contact",
                "status_1": "",
                "status_2": "Not Contacted",
            },
            "Contacted": {
                "item_status": "In Progress",
                "status_1": "Relevant Lead",
                "status_2": "Contacted/Follow-up",
            },
            "Qualified": {
                "item_status": "In Progress",
                "status_1": "Relevant Lead",
                "status_2": "Contacted/Follow-up",
            },
            "Junk Lead": {
                "item_status": "",
                "status_1": "Junk Lead",
                "status_2": "",
            },
            "Follow up": {
                "item_status": "In Progress",
                "status_1": "Relevant Lead",
                "status_2": "Contacted/Follow-up",
            },
            "No Interest": {
                "item_status": "",
                "status_1": "",
                "status_2": "Contacted/Lost",
            },
            "Not Contacted": {
                "item_status": "Not Contacted",
                "status_1": "",
                "status_2": "Not Contacted",
            },
            "Pre-checked": {
                "item_status": "Attempted to contact",
                "status_1": "",
                "status_2": "Not Contacted",
                "add_notes": {"Pre-checked": "Pre-checked"}
            }

        }
        if len(df) == 1:
            row = df.iloc[0]
            file_status = row["Status"] if not pd.isna(row["Status"]) else "Not Contacted"
            self.update_status_dict(file_status, file_status_mapping_dict)
        elif len(df) > 1:
            statuses = list(df["Status"].value_counts().sort_values().index)
            if len(statuses) == 1:
                file_status = statuses[0]
                self.update_status_dict(file_status, file_status_mapping_dict)
            elif len(statuses) == 2 and "Attempted to contact" in statuses and "Contacted" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Contacted": "Relevant Lead", "Attempted to contact": ""}
                self.statuses_dict["status_2"] = {"Contacted": "Contacted/Follow-up", "Attempted to contact": "Not Contacted"}
            elif len(statuses) == 3 and "No Interest" in statuses and "Contacted" in statuses and "Attempted to contact" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"No Interest": "", "Contacted": "Relevant Lead", "Attempted to contact": ""}
                self.statuses_dict["status_2"] = {"No Interest": "Contacted/Lost", "Contacted": "Contacted/Follow-up", "Attempted to contact": "Not Contacted"}
            elif len(statuses) == 3 and "Junk Lead" in statuses and "Contacted" in statuses and "Attempted to contact" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Junk Lead": "Junk Lead", "Contacted": "Relevant Lead", "Attempted to contact": ""}
                self.statuses_dict["status_2"] = {"Junk Lead": "", "Contacted": "Contacted/Follow-up", "Attempted to contact": "Not Contacted"}
            elif len(statuses) == 2 and "No Interest" in statuses and "Follow up" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Follow up": "Relevant Lead", "No Interest": ""}
                self.statuses_dict["status_2"] = {"Follow up": "Contacted/Follow-up", "No Interest": "Contacted/Lost"}
            elif len(statuses) == 2 and "Attempted to contact" in statuses and "Follow up" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Attempted to contact": "", "Follow up": "Relevant Lead"}
                self.statuses_dict["status_2"] = {"Attempted to contact": "Not Contacted", "Follow up": "Contacted/Follow-up"}
            elif len(statuses) == 2 and "Contacted" in statuses and "Junk Lead" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Contacted": "Relevant Lead", "Junk Lead": "Junk Lead"}
                self.statuses_dict["status_2"] = {"Contacted": "Contacted/Follow-up", "Junk Lead": ""}
            elif len(statuses) == 2 and "Attempted to contact" in statuses and "Not Contacted" in statuses:
                self.statuses_dict["item_status"] = "Attempted to contact"
                self.statuses_dict["status_1"] = {"Attempted to contact": "", "Not Contacted": ""}
                self.statuses_dict["status_2"] = {"Attempted to contact": "Not Contacted", "Not Contacted": "Not Contacted"}
            elif len(statuses) == 2 and "Qualified" in statuses and "Contacted" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Qualified": "Relevant Lead", "Contacted": "Relevant Lead"}
                self.statuses_dict["status_2"] = {"Qualified": "Contacted/Follow-up", "Contacted": "Contacted/Follow-up"}
            elif len(statuses) == 2 and "Not Contacted" in statuses and "Contacted" in statuses:
                self.statuses_dict["item_status"] = "In Progress"
                self.statuses_dict["status_1"] = {"Not Contacted": "", "Contacted": "Relevant Lead"}
                self.statuses_dict["status_2"] = {"Not Contacted": "Not Contacted", "Contacted": "Contacted/Follow-up"}
            elif len(statuses) == 2 and "Pre-checked" in statuses and "Junk Lead" in statuses:
                self.statuses_dict["item_status"] = "Attempted to contact"
                self.statuses_dict["status_1"] = {"Pre-checked": "", "Junk Lead": "Junk Lead"}
                self.statuses_dict["status_2"] = {"Pre-checked": "Not Contacted", "Junk Lead": ""}
                self.statuses_dict["add_notes"] = {"Pre-checked": "Pre-checked"}
            else:
                self.invalid_rows_df = pd.concat([df, self.invalid_rows_df])
                return None
        return self.statuses_dict

    def check_if_company_exists(self, company_name):
        company_item = """
            items_by_column_values(board_id: 4027051869, 
                column_id: "name", 
                column_value: "{company_name}"
                ) {response}
        """.format(
        company_name=company_name,
        response="""
            {
                id
                name,
                subitems {
                    id
                    name
                }
            }
        """
        )
        data_dict = {'query': "query {" + company_item + "}"}
        result = self.send_api_request(data_dict)
        return result

    def create_company_as_item(self):
        self.subitems_list = []
        self.company_email_list = []
        self.company_phone_list = []
        row = self.df.iloc[0]
        company_name = row["Company"].strip()
        result = self.check_if_company_exists(company_name)
        statuses_dict = self.get_board_status(self.df)
        if "data" in result and len(result["data"]["items_by_column_values"]) == 0:
            if len(self.df) > 1:
                company_revenue = ""
                """ 
                company_revenues = list(self.df["Company Revenue"].value_counts().sort_values().index)
                company_revenues.reverse()
                if len(company_revenues) > 0:
                    company_revenue = company_revenues[0]
                """
                location = ""
                locations = list(self.df["Location"].value_counts().sort_values().index)
                locations.reverse()
                if len(locations) > 0:
                    location = locations[0]
                branches = list(self.df["Branch"].value_counts().sort_values().index)
                branches.reverse()
                branches = "; ".join(branches)
                comment = ""
                comments = list(self.df["Comments"].value_counts().sort_values().index)
                comments.reverse()
                comment = "; ".join(comments)
                demo_date = ""
                demo_dates = list(self.df["Demo Date & Time"].value_counts().sort_values().index)
                demo_dates.reverse()
                if len(demo_dates) > 0:
                    demo_date = demo_dates[0]
                owner = ""
                owners = list(self.df["Owner"].value_counts().sort_values().index)
                owners.reverse()
                if len(owners) > 0:
                    owner = owners[0].lower()
                lead_source = ""
                if "Lead sources" in self.df.columns:
                    lead_sources = list(self.df["Lead sources"].value_counts().sort_values().index)
                    lead_sources.reverse()
                    if len(lead_sources) > 0:
                        lead_source = lead_sources[0]
            else:
                row = self.df.iloc[0]
                company_revenue = ""
                #company_revenue = row["Company Revenue"] if not pd.isna(row["Company Revenue"]) else ""
                branches = row["Branch"] if not pd.isna(row["Branch"]) else ""
                location = row["Location"] if not pd.isna(row["Location"]) else ""
                comment = row["Comments"] if not pd.isna(row["Comments"]) else ""
                demo_date = row["Demo Date & Time"] if not pd.isna(row["Demo Date & Time"]) else ""
                owner = row["Owner"].lower() if not pd.isna(row["Owner"]) else ""
                lead_source = ""
                if "Lead sources" in row:
                    lead_source = row["Lead sources"] if not pd.isna(row["Lead sources"]) else ""
            try:
                if demo_date:
                    datetime.strptime(demo_date, "%Y-%m-%d")
            except Exception:
                demo_date = ""  # demo date field must be in the special format, example "2023-01-18"
            self.df = self.df.apply(self.get_company_email_and_phone, axis=1)
            company_email = ""
            if len(self.company_email_list) > 0:
                company_email = self.company_email_list[0]
            row = self.df.iloc[0]
            if not statuses_dict:
                return None
            country = row["Country"] if not pd.isna(row["Country"]) else ""
            phone_company = ""
            if len(self.company_phone_list) > 0:
                phone_company = str(self.company_phone_list[0]).replace(" ", "").replace("-", "") if self.company_phone_list[0] != "n.v." else ""
            lost_reason = row["Lost Reasons"] if not pd.isna(row["Lost Reasons"]) else ""
            column_values = {
                "name": "",
                "phone_15": phone_company,
                "text1": country,
                #"location": location,
                "long_text": comment,
                "text09": lead_source,
                "date": demo_date,
                "text19": {"text": company_email, "email": company_email},
                "dropdown8": statuses_dict["item_status"],
                "numbers7": company_revenue,
                "branch": branches,
                "text53": lost_reason
            }
            owner_id = ""
            if owner in self.users_id_dict:
                owner_id = self.users_id_dict[owner]
            if owner_id:
                column_values["people"] = {"personsAndTeams": [{"id": owner_id, "kind": "person"}]}
            item_query = """
                create_item (
                    board_id: 4027051869,
                    group_id: "new_group84745",
                    item_name: "{company_name}",
                    column_values: {column_values}
                ) {response}
            """.format(
                column_values=json.dumps(json.dumps(column_values)),
                company_name=company_name,
                response="{id}"
            )
            data_dict = {'query': "mutation {" + item_query + "}"}
            result = self.send_api_request(data_dict)
            if "data" in result:
                parent_item_id = result["data"]["create_item"]["id"]
                return parent_item_id
            elif "error_code" in result:
                print(result)
                df_copy = self.df.copy()
                self.invalid_rows_df = pd.concat([self.invalid_rows_df, df_copy])
        elif "data" in result:
            parent_item_id = result["data"]["items_by_column_values"][0]["id"]
            subitems = result["data"]["items_by_column_values"][0]["subitems"]
            self.subitems_list = subitems if subitems else []
            return parent_item_id
        return None
    def create_users_dict(self):
        users_query = """ 
            users {
                name
                email
                id
            }
        """
        data_dict = {'query': "query {" + users_query + "}"}
        result = self.send_api_request(data_dict)
        if "data" in result:
            users_name_id_list = result["data"]["users"]
            for user_item in users_name_id_list:
                name = user_item["name"].lower()
                self.users_id_dict[name] = user_item["id"]

    def remove_suffix(self, company_name):
        if pd.isna(company_name):
            return ""
        words = ["posthausen", "group", "e-commerce", "holding", "tiernahrungs", "international", "retail", "tv", "icp", "handels", "( kg)", "deutschland", "türkiye", "fashion", "- coffee development", "global sales"]
        endswith_words = ["group", " se", "gruppe", " ag", "limited", " ltd", " uk", " plc", " & co", " and co"]
        for item in endswith_words:
            if company_name.endswith(item):
                company_name = company_name.replace(item, "")
        for item in words:
            if item in company_name:
                company_name = company_name.replace(item, "")
        company_name = company_name.replace("-", " ").replace("´", "").replace("'", "")
        return company_name.strip()
    def get_company_email_and_phone(self, row):
        email_company = "Email 1"  # second file
        phone_company = "Personal Phone 1"  # second file
        if not pd.isna(row[email_company]):
            email_part = row[email_company].split("@")[0]
            if any(email_str in email_part for email_str in self.general_email_company_list):
                if len(self.company_email_list) == 0:
                    self.company_email_list.append(row[email_company])
                    row[email_company] = ""
                if not pd.isna(row["Title"]) and any(title in row["Title"].lower() for title in self.title_ceo_list) and not pd.isna(row[phone_company]):
                    if len(self.company_phone_list) == 0:
                        self.company_phone_list.append(str(int(row[phone_company])))
                        row[phone_company] = ""
                return row
            return row
        return row

    def get_special_status(self, status_column, row_status):
        if type(self.statuses_dict[status_column]) == str:
            return self.statuses_dict[status_column]
        elif type(self.statuses_dict[status_column] == dict):
            return self.statuses_dict[status_column][row_status]

    def import_deals_to_board(self, file_path):
        dataframe = pd.read_excel(file_path)
        dataframe["Company"] = dataframe["Company"].str.strip()
        dataframe.sort_values(by=['Company'])
        dataframe["company_modified"] = dataframe["company_modified"].apply(self.remove_suffix)
        company_list = list(set(dataframe['company_modified'].tolist()))
        company_list.sort()
        self.create_users_dict()
        for company in company_list:
            self.company_modified = company
            self.df = dataframe[dataframe["company_modified"] == company].copy()
            parent_item_id = self.create_company_as_item()
            for index, row in self.df.iterrows():
                person_name = row["Name"] if not pd.isna(row["Name"]) else ""
                if not person_name:
                    continue
                if parent_item_id:
                    already_exist = False
                    for subitem in self.subitems_list:
                        if "name" in subitem:
                            contact_name = subitem["name"]
                            if person_name.lower().strip() == contact_name.lower().strip():
                                already_exist = True
                                break
                    if already_exist:
                        continue
                    person_name_list = list(map(str.strip, person_name.split(" ")))
                    person_first_name = ""
                    person_last_name = person_name
                    if len(person_name_list) >= 2:
                        person_first_name = person_name_list[0]
                        person_last_name = " ".join(person_name_list[1:])
                    if len(person_first_name) >= 2:
                        person_first_name = person_first_name.lower()
                        person_first_name = person_first_name[0].upper() + person_first_name[1:]
                    if len(person_last_name) >= 2:
                        person_last_name = person_last_name.lower()
                        person_last_name = person_last_name[0].upper() + person_last_name[1:]
                    person_name = person_first_name + " " + person_last_name
                    phone_person = str(int(row["Personal Phone 1"])).replace(" ", "").replace("-", "") if not pd.isna(row["Personal Phone 1"]) and row["Personal Phone 1"] != "" else ""   # "Phone(Person)" ?
                    phone_business = str(int(row["Personal Phone 2"])).replace(" ", "").replace("-", "") if not pd.isna(row["Personal Phone 2"]) and row["Personal Phone 2"] != "" else ""  # "Phone (Company)" ?
                    title = row["Title"] if not pd.isna(row["Title"]) else ""
                    owner = row["Owner"] if not pd.isna(row["Owner"]) else ""
                    email1 = row["Email 1"] if not pd.isna(row["Email 1"]) else ""
                    email2 = row["Email 2"] if not pd.isna(row["Email 2"]) else ""
                    status_1 = self.get_special_status("status_1", row["Status"])
                    status_2 = self.get_special_status("status_2", row["Status"])
                    notes = self.get_notes(row["Status"])
                    subitem_column_values = {
                        "name": person_name,
                        "text74": person_first_name,
                        "text7": person_last_name,
                        "phone": phone_business,
                        "phone9": phone_person,
                        "text": title,
                        "text1": notes,
                        "color": status_1,
                        "status": status_2,
                        "email": {"text": email1, "email": email1},
                        "email0": {"text": email2, "email": email2}
                    }
                    owner_id = ""
                    if owner.lower() in self.users_id_dict:
                        owner_id = self.users_id_dict[owner.lower()]
                    if owner_id:
                        subitem_column_values["person"] = {"personsAndTeams": [{"id": owner_id, "kind": "person"}]}
                    subitem_query = """
                        create_subitem (
                            parent_item_id: {parent_item_id},
                            item_name: "{name}",
                            column_values: {column_values}
                        ) {response}
                    """.format(
                        column_values=json.dumps(json.dumps(subitem_column_values)),
                        response="{id}",
                        parent_item_id=parent_item_id,
                        name=person_name
                    )
                    data_dict = {'query': "mutation {" + subitem_query + "}"}
                    result = self.send_api_request(data_dict)
                    if "error_data" in result:
                        invalid_df = dataframe[(dataframe["company_modified"] == company) & (dataframe["Name"] == person_name)]
                        self.invalid_rows_df = pd.concat([invalid_df, self.invalid_rows_df])
        if not self.invalid_rows_df.empty:
            self.invalid_rows_df = self.invalid_rows_df.drop(columns=["company_modified"])
            self.invalid_rows_df = self.invalid_rows_df.drop_duplicates()
            self.invalid_rows_df.to_excel(os.path.join(settings.ROOT_PATH, "monday_deals", "invalid_deals4.xlsx"))

    def get_notes(self, status_file):
        if "add_notes" in self.statuses_dict and status_file in self.statuses_dict["add_notes"]:
            return self.statuses_dict["add_notes"][status_file]
        return ""
