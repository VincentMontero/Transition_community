from crowd_utils import df_to_airtable, get_airtable, update_airtable_cell

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
    
def search_in_table(table, accepted_mail_list):
    my_json = table.all()
    url_linkedin_list = []

    for i in my_json:
        data = get_value(i, "fields")
        if data:
            email = get_value(data, "Email")
            for mail in accepted_mail_list:
                if email == mail:
                    url_linkedin_list.append(get_value(data, "linkedin_url"))
        # print(i, '\n')
    return url_linkedin_list

if __name__ == '__main__':
    table_answer = get_airtable("appHREIqHIs32toy0", "tblCs5fNMtxQb37RV")
    table_influenceur_1 = get_airtable("appHREIqHIs32toy0", "tblnSspDvQGVXMDiy")    
    table_influenceur_2 = get_airtable("appHREIqHIs32toy0", "tblP7p2ZShaLGQlfh")

    my_json = table_answer.all()
    status = ""
    accepted_mail_list = []

    for i in my_json:
        data = get_value(i, "fields")
        if data:
            # print(data, '\n')
            status = get_value(data, "Status")
        if data and status == "Accepté":
            accepted_mail_list.append(get_value(data, "Mail"))
            
    print(search_in_table(table_influenceur_1, accepted_mail_list))
    # search_in_table(table_influenceur_2, accepted_mail_list)
    print(accepted_mail_list)
