from pymongo import MongoClient
from pyairtable import Api

import sys
from sys import exit
from time import sleep

import pandas as pd 

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver

from bs4 import BeautifulSoup

from scripts_google_sheet import write_google_sheet, read_google_sheet, get_title_sheet, get_list_sheet_name

import os
import re

"""

This is a list of all the functions I have made while scraping in Collaboration Capital. It was hard sometimes even if the tasks were easy. Mostly because some websites are badly made.

"""


"""SCRAPPNIG UTILS"""
NOPRINT_TRANS_TABLE = {
    i: None for i in range(0, sys.maxunicode + 1) if not chr(i).isprintable()
}
SCROLL_PAUSE_TIME = 0.5

""" This deletes non-printable characters from a string, useful when there are \t or other characters """
def make_printable(s):
    return s.translate(NOPRINT_TRANS_TABLE)

""" Get wanted data from the dictionnary and insert it in the dataframe """
def stock_company_data(df, dico, COLUMNS):
    tmp = [dico[key] for key in COLUMNS]

    df.loc[len(df.index)] = tmp
    print(f"LISTE = {tmp}")
    return df

""" Get the substring after the delimiter """
def substring_after(s, delim):
    return make_printable(s.partition(delim)[2])

""" Get substring in s1 after delim in s2 """
def my_strstr(s1, s2):
    return make_printable(s1[s1.index(s2) + len(s2):])

""" This concatenates all the numbers in a list after start_index """
def concatenate_numbers_from_index(numbers, start_index):
    concatenated_number = ""
    
    if start_index < 0 or start_index >= len(numbers):
        return None
    
    for i in range(start_index, len(numbers)):
        concatenated_number += str(numbers[i])
    
    result = int(concatenated_number)
    return result

""" Finds links by string inside (instead use find_all_link_by_class) """
def find_links_in_webpage(soup, name):
    try:
        links = soup.find_all('a')
        
        all_links = [link.get('href') for link in links if link.get('href') and name in link.get('href')]
        
        return all_links
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

""" Remove a substring from a string """
def remove_substring(original_string, substring_to_remove):
    if substring_to_remove in original_string:
        updated_string = original_string.replace(substring_to_remove, "")
        return updated_string
    else:
        return original_string

""" Use when using selenium, this dezooms the page """
def dezoom(driver, n):
    driver.set_context("chrome")
    win = driver.find_element(By.TAG_NAME,"html")
    for i in range(n):
        win.send_keys(Keys.CONTROL + "-")
    driver.set_context("content")

""" Same as dezoom... but zoom """
def zoom(driver, n):
    driver.set_context("chrome")
    win = driver.find_element(By.TAG_NAME,"html")
    for i in range(n):
        win.send_keys(Keys.CONTROL + "+")
    driver.set_context("content")

""" Use when using selenium, this scrolls the page at n"""
def scroll(driver, n):
    driver.execute_script("window.scrollTo(0, {i})".replace("{i}", str(n)))
    sleep(3)

""" Scroll d'un coup vers le bas d'une page """
def scroll_to_bottom(driver):
    prev_height = driver.execute_script("return Math.max( document.body.scrollHeight, document.body.offsetHeight, document.documentElement.clientHeight, document.documentElement.scrollHeight, document.documentElement.offsetHeight );")

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        driver.implicitly_wait(5)

        new_height = driver.execute_script("return Math.max( document.body.scrollHeight, document.body.offsetHeight, document.documentElement.clientHeight, document.documentElement.scrollHeight, document.documentElement.offsetHeight );")
        if new_height == prev_height:
            break
        prev_height = new_height

""" Scroll progressif vers le bas d'une page (le plus souvent celui à utiliser car sites dynamiques de scrolling) """
def scroll_down(driver):

    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    
        sleep(SCROLL_PAUSE_TIME)
    
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

""" Gets the first number found in a string """
def get_nbr(string):
    string = string.replace(",", ".")
    return int(re.sub("[^0-9]", "", string))

""" Gets the first number found in a string, but useful for american notation of numbers. """
def get_nbr_comma(string):
    string = string.replace(",", ".")
    match = re.search(r"\d+", string)
    if match:
        return int(match.group())
    else:
        return None

""" Find all numbers in a string. returns a list of nb """
def find_numbers_in_string(input_string):
    numbers = re.findall(r'\d+(?:\s*\d+)*', input_string)
    return numbers

""" Gets the first number in a string"""
def get_first_number(text):
    pattern = r'\d+'
    match = re.search(pattern, text)
    
    if match:
        first_number = match.group()
        return int(first_number)
    else:
        return None

""" Returns the percentage """
def get_percentage(current, wanted):
    return (100 * (int(current) / int(wanted)))

""" This should remove all spaces but one. """
def remove_spaces(string):
    return re.sub(r'\s+', ' ', string)

#get_create_sheet pour créer un nouvel onglet
""" I used this to write inside a df and keeping the content of the old df, don't use it """
def verif_GS(df, URL, index):
    title = get_title_sheet(URL)
    onglets = get_list_sheet_name(URL)
    old_df = read_google_sheet(URL, onglets[index])
    
    print(f"Title: {title}, onglet: {onglets[index]}")
    concat_df = pd.concat([old_df, df])
    print(concat_df)
    write_google_sheet(concat_df.dropna(how='all'), URL, onglets[index])
    
    
#get_create_sheet pour créer un nouvel onglet 
""" Used to write inside a GS, you need a credentials.json file (you can create one for yourself), you can change the onlets by the string you want, or use the index like I do. """
def write_GS(df, URL, index):
    title = get_title_sheet(URL)
    onglets = get_list_sheet_name(URL)
    
    print(f"Title: {title}, onglet: {onglets[index]}")
    write_google_sheet(df.dropna(how='all'), URL, onglets[index])

"""body, tag, ttype, name"""
""" Used to find one element in a body (use with beautifulsoup) """
def find_single(body, tag, ttype, name):
    try:
        dest = make_printable(body.find(tag, {ttype:name}).text.strip())
    except:
        dest = None
        pass
    return dest

""" Used to find all elements with a certain ttype and name, it returns a list. Use with beautifulsoup """
def find_all(body, tag, ttype, name):
    try:
        dest = body.find_all(tag, {ttype:name})
    except Exception as e:
        print(e)
        dest = None
        pass
    return dest

""" This finds one link by its class. Use with beautifulsoup """
def find_link_by_class(soup, tag, ttype, name):
    elem = soup.find(tag, {ttype:name})

    if elem:
        return elem.get('href')

    return None

""" This finds all links by class, use with beautifulsoup """
def find_all_link_by_class(soup, tag, ttype, name):
    elem = soup.find_all(tag, {ttype: name})
    myList = []

    for links in elem:
        myList.append(links.get('href'))

    return myList

""" Same as remove_substring, but better. """
def remove_substring_reg(main_string, substring):
    result_string = re.sub(re.escape(substring), '', main_string)
    return result_string

""" This gets a mongodb database and returns a client, you can then get the collection. """
def get_database(str):
    client = MongoClient("localhost", 27017)
    if (str not in client.list_database_names()):
        exit(-1)
 
    return client[str]

""" prints the list of all collections in mongodb """
def get_collection_list(db):
    print(db.list_collection_names())

""" Insert a document inside a collection. mongodb """
def insert_data(collection, query):
    return (collection.insert_one(query))

""" Insert a list of documents inside a collection (you can transform a df into a dict, try it) """
def insert_list(collection, query_list):
    return (collection.insert_many(query_list))

""" This insert a dataframe inside a collection, useful when you want to add everything in one go. """
def df_to_db(df, collection):
    collection.insert_many(df.to_dict('records'))
 
""" Insert a dataframe inside a table from airtable. See pyairtable documentation to add only one element, it's not hard """
def df_to_airtable(df, table):
    df.fillna("", inplace=True)

    dico_table = df.to_dict('records')
    table.batch_create(dico_table)

""" Obtenir le tableau de la Airtable avec l'app id et le tblid (dans l'URL, appID/tblID/viewID)"""
""" You need to set the "AIRTABLE_TOKEN" environment variable, don't forget. """
def get_airtable(app_id, tbl_id):
    api = Api(os.environ['AIRTABLE_TOKEN'])
    
    return api.table(app_id, tbl_id)

""" Update une case d'une table airtable (l'id commençant par rec) """
""" L'argument table peut s'obtenir avec get_airtable, le rec_id est soit dans l'URL, soit c'est l'élément 'id' quand on insère un élément, ensuite cell et content sont des strings (le nom d'une colonne, et le contenu) """
def update_airtable_cell(table, rec_id, cell, content):
    table.update(rec_id, {cell: content})

""" Find element in mongodb collection """
def find_in_col(collection, query):
    for x in collection.find({}, query):
        print(x)

""" Gets all the collections and inserts them inside a Dataframe, cool isn't it ? """
def get_collection_df(collection):
    try:
        df = pd.DataFrame(list(collection.find({})))
    except:
        df = pd.DataFrame()
        pass
    return df

""" Check if your URL source is inside the old_df (made from get_collection_df), we used this to check if we already scraped the website, so we would not do it again, that's useful as an optimization. To get the url using selenium, you can do driver.current_url | when using requests, you should already have the url of the page you're currently in. """
def check_src_in_df(df, source):
    try:
        for index, line in df.iterrows():
            if (line["URL source"] == source):
                return index, line
    except:
        pass
    return None, None

""" Like check_src_in_df but with pd.NA, I personally don't use it. """
def is_source_in_df(df, source):
    try:
        for index, line in df.iterrows():
            if (line["URL source"] == source):
                return index, line
    except:
        pass
    return pd.NA, pd.NA

""" I don't use it but it's a little function to get the body of a page. it's cool """
def get_body(source):
    body = BeautifulSoup(source, features="lxml").find("body")
    return body
      
""" Used with get_all_links, it checks if it's the bottom of a page. """  
def is_bottom(driver):
    body1 = get_body(driver.page_source).text
    scroll_down(driver)
    body2 = get_body(driver.page_source).text

    return body1 == body2

""" Have a page where there's a dynamic scrolling and you have a class for each companies you want to click on ? Use this. If you have an id or any other identifier used inside the HTML, modify the function. This is really useful. """
""" This function will return a list of all the links found in the body that have the class post__link, obviously it will not work if the class is different or there's no class at all, you may need to adapt it. If you can't use it, good luck. """
def get_all_links(URL):
    links = []
    
    driver = webdriver.Firefox()
    driver.get(URL)
    dezoom(driver, 10)
    sleep(3)
    
    print("Getting links...")
    while (True):
        scroll_down(driver)
        if is_bottom(driver):
            print("Can't scroll more : bottom reached.")
            break
    
    body = get_body(driver.page_source)
    driver.quit()
    if body == None:
        return links
    
    post_links = body.find_all("a", {"class" : "post__link"}) # modify this if needed
    for post in post_links:
        if post.has_attr("href"):
            link = post["href"]
            if link not in links:
                links.append(link)
    
    print(f"{len(links)} links found.")
    return links

""" Same as get_all_links but you need to specify ttype and name. """
def get_all_links_selenium(URL, ttype, name):
    links = []
    driver = webdriver.Firefox()
    driver.get(URL)
    dezoom(driver, 10)
    sleep(2)
    
    print("Getting links...")
    while (True):
        scroll_down(driver)
        if is_bottom(driver):
            print("Can't scroll more : bottom reached.")
            break
    
    body = get_body(driver.page_source)
    driver.quit()
    if body == None:
        return links
    
    post_links = body.find_all("a", {ttype: name})
    for post in post_links:
        if post.has_attr("href"):
            link = post["href"]
            if link not in links:
                links.append(link)
    
    print(f"{len(links)} links found.")
    return links
