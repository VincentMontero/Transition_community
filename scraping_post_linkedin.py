import pandas as pd
import requests
from scripts_google_sheet import write_google_sheet, read_google_sheet, get_title_sheet, get_list_sheet_name
from crowd_utils import df_to_airtable, get_airtable, update_airtable_cell

URL = "https://docs.google.com/spreadsheets/d/1-36-vEgVeCmCeAxJ7LlPnZ7x98Vqda5AtXGAAgFM_fw/edit#gid=458946538"

if __name__ == '__main__':
    #Ce script sert à envoyer les influenceurs du GS dans un fichier que j'utilise pour récupérer les posts, j'envoie aussi certaines colonnes du GS dans mes tables Airtable.
    ##S'il est relancé, il y aura des doublons dans les tables, ne pas le faire (ou enlever les df_to_airtable)
    """
    f = open("linkedin_profiles.txt", "w")
    GS_df = read_google_sheet(URL, "Onglet - 2 - ")
    GS_df_2 = read_google_sheet(URL, "Onglet - 6 - ")
    
    filtered_df = GS_df[GS_df["Nombre d'abonnés"] >= 1000]
    filtered_df_2 = GS_df_2[GS_df_2["Nombre d'abonnés"] >= 1000]
    for index, row in filtered_df.iterrows():
        try:
            f.write(row["linkedin_url"])
            f.write('\n')
        except:
            pass
    f.close()
    
    f = open("linkedin_profiles.txt", "a")
    
    for index, row in filtered_df_2.iterrows():
        try:
            f.write(row["linkedin_url"])
            f.write("\n")
        except:
            pass
    f.close()

    wanted_columns = ["full_name", "Nombre d'abonnés", "Email", "1er envoi", "Email erroné", "2nd Envoi", "linkedin_url"]
    new_df = filtered_df[wanted_columns]
    new_df_2 = filtered_df_2[wanted_columns]
    table_influenceurs = get_airtable("appHREIqHIs32toy0", "tblnSspDvQGVXMDiy")
    table_influenceurs_2 = get_airtable("appHREIqHIs32toy0", "tblP7p2ZShaLGQlfh")
    
    df_to_airtable(new_df.copy(), table_influenceurs)
    df_to_airtable(new_df_2.copy(), table_influenceurs_2)

    df_answer = read_google_sheet(URL, "Réponses")
    wanted_columns = ["Prenom", "Nom", "Mail", "url linkedin / post", "Observation"]
    new_df_answer = df_answer[wanted_columns]
    
    table_answer = get_airtable("appHREIqHIs32toy0", "tblCs5fNMtxQb37RV")
    df_to_airtable(new_df_answer.copy(), table_answer)
    """