from functions import *


def main(words_near_list, distance, start_date, end_date, save_as_txt, print_excerpt, length_excerpt, fuzziness, directory):
    # making sure the dates are yyyy-mm-dd format
    start_date, end_date, fuzziness, save_as_txt = clean_input_general(start_date, end_date, fuzziness,
                                                                       save_as_txt)  # getting the formatting right
    check_validity_of_input(start_date, end_date, fuzziness, words_near_list)
    possible_files, vergaderjaren_unspecified_dates = list_of_debates(start_date, end_date)



    search_terms = make_list_of_search_terms(words_near_list)  # puts all relevant n-grams in one list
    words_near_list = clean_words_near_list(words_near_list)  # replaces spaces with _ in the query input
    year = False
    file_output_counter = 0
    searchterms_output_counter = 0
    total_words = 0
    datarows = []
    start_txt_file({"words_near_list": words_near_list, "distance": distance, "start_date": start_date, "end_date":
                    end_date,"fuzziness": fuzziness}, save_as_txt)

    for file_name in possible_files:
        contents_split = None
        year = print_and_save_new_year(year, file_name)  # makes sure we get a print when we start a new year
        try:
            contents = create_contents(directory, file_name)
        except MemoryError:
            raise MemoryError(f"memory error in {file_name}")
        contents = replace_spaces_in_contents(contents, search_terms)
        datarow = generate_data_row(contents, words_near_list, distance, contents_split,fuzziness, search_terms,
                                    length_excerpt)
        if datarow is None:
            continue

        datarow["source"], datarow['url'] = key_value_to_printable_and_URL(file_name, False, '', vergaderjaren_unspecified_dates=vergaderjaren_unspecified_dates)

        # printing the output as we go along
        try:
            printable = print_data_row(datarow, print_excerpt)
        except TypeError:
            printable = print_data_row(datarow)

        # saving the output in the text file
        if save_as_txt:
            with open(save_as_txt, 'a') as f:
                f.write(printable)

        file_output_counter += 1
        print(file_output_counter)
        if datarow['nr_of_searchterms_found'] is not None:
            searchterms_output_counter += datarow['nr_of_searchterms_found']

    stats_result = "number of documents found:", file_output_counter
    stats_result = f"{stats_result}\nnumber of searchterms found: {searchterms_output_counter}"
    print(stats_result)
    with open(save_as_txt, 'a') as f:
        f.write(stats_result)
