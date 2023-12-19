from crowd_utils import df_to_airtable, get_airtable, update_airtable_cell
from time import sleep

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

def count_words_in_text(text, word_list):
    """
    Count the number of times words from the word_list appear in the text.

    Parameters:
    - text (str): The input text.
    - word_list (list): List of words to check for in the text.

    Returns:
    - int: Number of occurrences of words from the word_list in the text.
    """
    text_lower = text.lower()

    word_counts = {word: text_lower.count(word.lower()) for word in word_list}

    total_occurrences = sum(word_counts.values())
    return total_occurrences

def compute_score(table, table_words, text):
    my_json = table.all()
    transition_word_list = []
    score = 0

    for words in table_words.all():
        data = get_value(words, "fields")
        if data:
            transition_word_list.append(get_value(data, "Name"))

    for i in my_json:
        rec_id = get_value(i, 'id')
        print(f"The id is: {rec_id}")
        data = get_value(i, "fields")
        if data:
            article = get_value(data, text)
            if article:
                score = count_words_in_text(article, transition_word_list)
                print(f"The score of this article is: {score}\n")
                print(article, "\n\nNOUVEAU POST ------------\n")
                update_airtable_cell(table, rec_id, "Score", score)
                # sleep(1)

if __name__ == '__main__':
    table_linkedin = get_airtable("appHREIqHIs32toy0", "tblpzRA3cVfKynnmA")
    table_article = get_airtable("appHREIqHIs32toy0", "tblvM3eejH1sjlraT")
    table_words = get_airtable("appHREIqHIs32toy0", "tblFdaPIrn966NxMv")
    
    compute_score(table_article, table_words, "summary_text")
    """
    my_json = table_linkedin.all()
    transition_word_list = []
    score = 0
    
    
    for words in table_words.all():
        data = get_value(words, "fields")
        if data:
            transition_word_list.append(get_value(data, "Name"))

    for i in my_json:
        rec_id = get_value(i, 'id')
        print(f"The id is: {rec_id}")
        data = get_value(i, "fields")
        if data:
            article = get_value(data, "content_text")
            if article:
                score = count_words_in_text(article, transition_word_list)
                print(f"The score of this article is: {score}\n")
                print(article, "\n\nNOUVEAU POST ------------\n")
                update_airtable_cell(table_linkedin, rec_id, "Score", score)
                # sleep(1)
    """
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            