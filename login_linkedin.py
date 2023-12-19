import requests
import json
import os
import pandas as pd
from crowd_utils import df_to_airtable, get_airtable, update_airtable_cell
from scripts_google_sheet import write_google_sheet, read_google_sheet, get_title_sheet, get_list_sheet_name
from compute_score import compute_score

headers = {
        'x-api-key': os.environ['CAPTAINDATA_TOKEN'],
        'x-project-id': 'de9fe3bb-13b6-49e9-bea8-1dab08b6e414'
}

def get_value(json_data, key):
    try:
        if isinstance(json_data, list):
            first_element = json_data[0]
            value = first_element[key]
        elif isinstance(json_data, dict):
            value = json_data[key]
        else:
            print("Invalid JSON data type. Supported types: list or dict.")
            return None

        return value
    except (KeyError, IndexError, TypeError):
        print(f"Key '{key}' not found in the JSON data: {json_data}")
        return None
    
def get_nested_value(json_data, keys):
    try:
        for key in keys:
            if isinstance(json_data, list):
                first_element = json_data[0]
                json_data = first_element[key]
            elif isinstance(json_data, dict):
                json_data = json_data[key]
            else:
                print("Invalid JSON data type. Supported types: list or dict.")
                return None

        return json_data
    except (KeyError, IndexError, TypeError):
        print(f"Keys '{keys}' not found in the JSON data.")
        return None

def get_url_from_file(url, nb_url):
    i = 0
    list_lien = []

    with open(url, 'r') as file:
        for line in file:
            if i == nb_url:
                break
            list_lien.append(line.strip())
            i += 1

    return list_lien
            
def get_all_url_from_file(url):
    list_lien = []

    with open(url, 'r') as file:
        for line in file:
            list_lien.append(line.strip())

    return list_lien

def launch_workflow(workflow_uid, file_input_url):
    url_api = "https://api.captaindata.co/v3/workflows/{WORKFLOW_UID}/schedule".replace("WORKFLOW_UID", workflow_uid)
    list_lien = get_all_url_from_file(file_input_url)
    input_cd = [{"linkedin_profile_url" : lien} for lien in list_lien]
    
    return url_api, input_cd

def get_last_workflow_result(workflow_uid):
    url_api = "https://api.captaindata.co/v3/workflows/{WORKFLOW_UID}/results/last".replace("WORKFLOW_UID", workflow_uid)
    
    response = requests.get(url_api, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def retry_job(job_uid, payload):
    url_api = "https://api.captaindata.co/v3/jobs/{JOB_UID}/retry".replace("JOB_UID", job_uid)
    
    response = requests.post(url_api, headers=headers, data=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def put_users_in_file_df(URL_GS, filename):
    f = open(filename, "w")
    GS_df = read_google_sheet(URL_GS, "Onglet - 2 - ")
    
    filtered_df = GS_df[GS_df["Nombre d'abonnés"] >= 1000]
    
    for index, row in filtered_df.iterrows():
        try:
            f.write(row["linkedin_url"])
            f.write('\n')
        except:
            pass
    f.close()
    
def put_users_in_file_list(my_list, filename):
    f = open(filename, "w")
    
    
    for elem in my_list:
        try:
            f.write(elem)
            f.write('\n')
        except:
            pass
    f.close()
    
def search_in_table(table, accepted_mail_list):
    my_json = table.all()
    url_linkedin_list = []

    for i in my_json:
        data = get_value(i, "fields")
        if data:
            email = get_value(data, "Email")
            for mail in accepted_mail_list:
                print(email, mail, '\n')
                if email == mail:
                    url_linkedin_list.append(get_value(data, "linkedin_url"))
        # print(i, '\n')
    return url_linkedin_list

def post_on_airtable(put_on_airtable: bool):
    table_posts = get_airtable("appHREIqHIs32toy0", "tblpzRA3cVfKynnmA")
    table_words = get_airtable("appHREIqHIs32toy0", "tblFdaPIrn966NxMv")
    
    my_json = get_last_workflow_result("60eb1d78-b7fb-47cb-99d0-b396599067e9")
    
    results = get_value(my_json, "results")
    columns = ["full_name", "content_text", "linkedin_post_url", "linkedin_profile_url", "linkedin_post_id", "Published_time"]
    df = pd.DataFrame(columns=columns)

    for result in results:
        tmp = [get_value(result, col) for col in columns]
        # tmp2 = [get_value(result, "full_name"), get_value(result, "linkedin_profile_url")]

        df.loc[len(df.index)] = tmp
    
    if put_on_airtable:
        df_to_airtable(df, table_posts)
        compute_score(table_posts, table_words, "summary_text")

    print(df)

    return df

if __name__ == '__main__':
    table_answer = get_airtable("appHREIqHIs32toy0", "tblCs5fNMtxQb37RV")
    table_influenceur_1 = get_airtable("appHREIqHIs32toy0", "tblnSspDvQGVXMDiy")
    table_influenceur_2 = get_airtable("appHREIqHIs32toy0", "tblP7p2ZShaLGQlfh")
    my_json = table_answer.all()
    json_infl_1 = table_influenceur_1.all()
    json_infl_2 = table_influenceur_2.all()

    status = ""
    accepted_mail_list = []
    in_table_mail_list = []
    
    """ Verif que mail soit pas déjà dans une table TODO: mettre ça dans une fonction """
    for infl in json_infl_1:
        data = get_value(infl, "fields")
        if data:
            mail = get_value(data, "Email")
            if mail:
                in_table_mail_list.append(mail)
    for infl in json_infl_2:
        data = get_value(infl, "fields")
        if data:
            mail = get_value(data, "Email")
            if mail:
                in_table_mail_list.append(mail)
    
    for i in my_json:
        data = get_value(i, "fields")
        if data:
            # print(data, '\n')
            status = get_value(data, "Status")
            mail = get_value(data, "Mail")
        if data and status == "Accepté" and mail not in in_table_mail_list:
            accepted_mail_list.append(get_value(data, "Mail"))

    """ Début de recherche des influenceurs puis captain data recherche posts"""
    filename = "url_linkedin_accepted.txt"
    accepted_url = search_in_table(table_influenceur_1, accepted_mail_list)
    accepted_url += search_in_table(table_influenceur_2, accepted_mail_list)
    put_users_in_file_list(accepted_url, filename)

    ## RECUP DES POSTS AVEC l'API CAPTAIN DATA
    url_api, input_cd = launch_workflow("60eb1d78-b7fb-47cb-99d0-b396599067e9", filename)
    linkedin_account = "f8ae1c5b-2f6e-40c6-9ec0-9ce3dd683388"

    my_data = {
      "steps": [
        {
          "accounts": [linkedin_account],
          "parameters": {"max_results": 10},
          "accounts_rotation_enabled": False,
          "step_uid": "c8cba9c6-ac0b-4d5f-be9e-db1e93b4fc88"
        }
      ],
      "unstructure_meta": False,
      "inputs": input_cd,
      "job_name": "Extract LinkedIn People Post Activity"
    }
    
    payload = json.dumps(my_data)

    response = requests.request("POST", url_api, headers=headers, data=payload)
    
    try:
        print(response.json())
    except:
        print("No.")
    """ Post sur airtable """
    post_on_airtable(False) #Mettre en True aorès que la première partie aie été lancée (la commenter pour éviter de tout relancer)
    
    ## print(retry_job("95ec650b-5b8e-421a-b139-6c6884885dd1", payload))
