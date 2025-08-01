import pandas as pd
from rapidfuzz import fuzz
import re

def clean_input_general(start_date, end_date, fuzziness, save_as_txt):
	start_date = fix_date(start_date)
	end_date = fix_date(end_date)
	if save_as_txt:
		if not save_as_txt.endswith(".txt"):
			save_as_txt = f"{save_as_txt}.txt"
	if isinstance(fuzziness, dict):
		new_approximate_match = {}
		for key, value in fuzziness.items():
			if value == 100:
				continue

			if isinstance(key, tuple):
				for item in key:
					new_approximate_match[item.upper()] = value
			else:
				new_approximate_match[key.upper()] = value
		fuzziness = new_approximate_match
	return start_date, end_date, fuzziness, save_as_txt


def fix_date(date):
	"""function that reformulates dates into a yyyy-mm-dd format. requires calendar if converting written out months in Dutch or English (not every configuration in English supported"""
	if date is None:
		return None
	date = date.replace("\n", " ")
	date = date.strip()
	date = date.lower()
	while "  " in date:
		date = date.replace("  ", " ")
	date = date.replace("/", "")
	date = date.replace(" ", "-")
	months_abbreviations = {"jan-": "januari-", "feb-": "februari-", "maa-": "maart-", "apr-": "april-", "jun-": "juni-", "jul-": "juli-", "aug-": "augustus-", "sep-": "september-", "okt-": "oktober-", "nov-": "november-", "dec-": "december-"}
	for key, value in months_abbreviations.items():
		date = date.replace(key, value)
	date = date.replace(",", "")
	if not date.replace("-", "").isdigit():
		import calendar
		import locale
		if "-" not in date:
			raise ValueError(f"invalid input for date, your input is {date}")
		date_parts = date.split("-")

		maand_uitgeschreven = date_parts[1]
		try: #in case of Dutch month names
			locale.setlocale(locale.LC_ALL, 'NL_nl')
			date = date.replace(maand_uitgeschreven, str(list(calendar.month_name).index(maand_uitgeschreven)))
		except ValueError: #in case of English month names
			locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
			date = date.replace(maand_uitgeschreven, str(list(calendar.month_name).index(maand_uitgeschreven.title())))

	if date[:4].isdigit() and date[4] == "-" and date[-4:].isdigit():
		return date
	if date[4] != "-" or date[7] != "-" or len(date) != 10: #if all three are the case, our formatting is normal, and we don't need to do anything
		if date[:4].isdigit() and date.count("-") == 2:
			units = date.split("-")
			day = units[2]
			month = units[1]
			year = units[0]
			print(year, month, day)

		elif date[-4:].isdigit() and date.count("-") == 2:
			units = date.split("-")
			day = units[0]
			month = units[1]
			year = units[2]

		else:
			raise ValueError("invalid input for date! your input is", date)

		if int(day) > 31:
			raise ValueError(f"input for day of greater than 31! Your input is {date}")
		if int(month) > 12:
			print(date)
			raise ValueError(f"input for month is greater than 12! Your input is {date}")
		if len(day) < 2:
			day = f"0{day}"
		if len(month) < 2:
			month = f"0{month}"

		date = f"{year}-{month}-{day}"
	return date


def check_validity_of_input(start_date, end_date, approximate_match, words_near_list):
	if start_date:
		start_date_num = int(start_date.replace("-", ""))
	if end_date:
		end_date_num = int(end_date.replace("-", ""))
	if isinstance(approximate_match, int):
		if approximate_match == 100:
			print("you specified approximate_match as 100. Input False for the same results, but much faster")
		elif approximate_match > 100:
			raise ValueError(f"You input approximate_match {approximate_match}. Please specify an approximate_match smaller than 100.")
	elif isinstance(approximate_match, dict):
		for match_value in approximate_match.values():
			if match_value > 100:
				raise ValueError(f"you input an approximate_match value of {match_value}. Please specify a value smaller than 100")
	else:
		raise ValueError(f"you input {type(approximate_match)} for approximate_match, please put int or dic")

	if start_date and end_date:
		if start_date > end_date:
			raise ValueError("you specified a start date that is later than your end date")
	if words_near_list:
		if len(words_near_list) != 2 and len(words_near_list) != 3:
			raise ValueError(f"words_near_list should contain 2 or 3 items, instead it contains {len(words_near_list)} items")

def list_of_debates(start_date, end_date):
	"""returns a list of files that correspond with the metadata input"""
	dates_files_correct_metadata = []
	if start_date:
		start_date = int(start_date.replace("-", ""))
	if end_date:
		end_date = int(end_date.replace("-", ""))
	path = "metadata15.parquet"
	filters_input = []
	if start_date:
		filters_input.append(("Datum", ">=", start_date))
	if end_date:
		filters_input.append(("Datum", "<=", end_date))
	relevant_columns = ["filename", "Datum", "Vergaderjaar"]
	df = pd.read_parquet(path, columns=relevant_columns, filters=[filters_input])
	# we are now going to apply filters that are too complicated to directly input them into parquet in the line above.
	df = df[
		(~df['filename'].str.contains('register'))  # register files are too heavy to really work with
		]

	vergaderjaren_unspecified_dates = {}
	df['filename'] = df['filename'].map(lambda x: f'{x}.txt')
	df = df.sort_values(by="Datum")
	dates_files_correct_metadata = list(df['filename'])
	df['identifier_str'] = df['filename'].str[-14:-4]

	#some files are undated, but we do have a parliamentary year (vergaderjaar), so that is used to estimate a date.
	vergaderjaren_unspecified_dates = dict(zip(df['identifier_str'], df['Vergaderjaar']))  # this is a dictionary with all files (i.e. not just those without dates) with identifier and vergaderjaar, so that we can later insert vergaderjaar into our print

	if len(vergaderjaren_unspecified_dates) == 0:
		vergaderjaren_unspecified_dates = False

	return dates_files_correct_metadata, vergaderjaren_unspecified_dates

def make_list_of_search_terms(words_near_list):
	search_terms = []
	if words_near_list:
		for categorie in words_near_list:
			search_terms += str_to_list(categorie)
	if not isinstance(search_terms, list):
		raise TypeError(f"search_terms should be list, not {type(search_terms)}")
	search_terms = [term.upper().replace(" ", "_") for term in search_terms]
	return search_terms

def clean_words_near_list(words_near_list):
	new_words_near_list = []
	for categorie in words_near_list:
		categorie = str_to_list(categorie)
		categorie = [item.replace(" ", "_").upper() for item in categorie]
		new_words_near_list.append(categorie)
	words_near_list = new_words_near_list
	return words_near_list

def start_txt_file(arguments, save_output_as_txt):
	future_contents = "search results for the query:"
	if not save_output_as_txt:
		return
	for value_name, value_contents in arguments.items():
		if value_contents:
			future_contents = f"{future_contents}\n{value_name}: {value_contents}"

	future_contents = f"{future_contents}\n\n"
	with open(f'{save_output_as_txt.replace(".txt", "")}.txt', 'w') as f:
		f.write(future_contents)


def print_and_save_new_year(year, file_name):
	year_file = file_name[:4]
	if not year_file.isdigit():
		if file_name.startswith("date unspecified"):
			return year
		raise ValueError(f'filename {file_name} does not start with a year')
	if year_file == year:
		return year
	else:
		print(year_file)
		return(year_file)


def create_contents(directory, file_name):
	"""creates contents, by either opening a txt file, or by compiling everything said by specific speaker in XML file. Function formerly called open_file()"""
	path = f"{directory}/{file_name}"

	try:
		with open(path, 'r', encoding="utf8") as file:
			contents = file.read()
	except UnicodeDecodeError:
		with open(path, 'r') as file:
			contents = file.read()
	except FileNotFoundError:
		try:
			with open(path.replace(" - ", "-"), 'r', encoding="utf8") as file:
				contents = file.read()
		except FileNotFoundError:  # I was not consistent with whether I saved the pagenumbers of files with 1 page number as "333" or "333 - 333". This fixes that
			path_list = path.split(" ")
			path_string = ""
			recent_hyphen = False
			for i in range(0, len(path_list)):
				if path_list[i] == "-":
					recent_hyphen = True
					continue
				elif recent_hyphen:
					recent_hyphen = False
					continue
				else:
					path_string = f"{path_string} {path_list[i]}"
			path_string = path_string.lstrip()
			path = path_string
			try:
				with open(path, 'r', encoding="utf8") as file:
					contents = file.read()
			except FileNotFoundError:
				print("ATTENTION! this file doesn't seem to exist:", path)
				contents = ""

	contents = contents.replace("(cid:173)\n", "")  # (cid:173) is a code for a hyphen.
	contents = contents.replace("-\n", "")
	contents = contents.upper()

	return contents

def replace_spaces_in_contents(contents, search_terms):
	"""replaces spaces in the terms in the query for _'"""
	search_terms = str_to_list(search_terms)
	for term in [term for term in search_terms if " " in term]:
		term = term.upper()
		term_spaceless = term.replace(" ", "_")
		contents = contents.replace(term, term_spaceless)
	return contents


def generate_data_row(contents, words_near_list, distance, contents_split,fuzziness, search_terms, length_excerpt):
	"""takes contents and input and returns None if conditions are not met, or a datarow if conditions are met"""
	datarow = {"source":None, 'url':None, "excerpts": None, "nr_of_searchterms_found": None, "extra_prints": None}

	if words_near_list:
		excerpts, nr_of_searchterms_found = words_near(contents, words_near_list[0], words_near_list[1], distance, fuzziness = fuzziness)
		if len(excerpts) == 0:
			return None
		datarow['nr_of_searchterms_found'] = nr_of_searchterms_found
		datarow['excerpts'] = excerpts
	return datarow

def words_near(contents, searchterm0, searchterm1, distance, fuzziness = False):
	"""Finds files in which two or three words appear near eachother"""
	searchterm0 = str_to_list(searchterm0)
	searchterm1 = str_to_list(searchterm1)
	excerpt_list = check_near_list_or_string(contents, searchterm0, searchterm1, distance, fuzziness =fuzziness)
	exerpts_as_string = "  ".join(excerpt_list)
	searchterm_counter = 0
	for term in searchterm0 + searchterm1:
		searchterm_counter += exerpts_as_string.count(term)
	return excerpt_list, searchterm_counter

def str_to_list(str_or_list):
	"""checks if input is a string or list. If it is a string, converts it to list"""
	if isinstance(str_or_list, str):
		str_or_list = [str_or_list]
	return str_or_list


def check_near_list_or_string(string_or_list, searchterm0, searchterm1, distance, fuzziness=False):
	"""checks whether two words appear near eachother in a string or list. Distance is number of characters in between the words (so excluding the words themselves)"""
	# making lists out of possible str searchterms
	searchterm0 = str_to_list(searchterm0)
	searchterm1 = str_to_list(searchterm1)
	searchterms = searchterm0 + searchterm1
	matches0 = []
	matches1 = []
	# finding areas with all two or three words in it
	areas_with_words_list = []
	if isinstance(string_or_list, list):
		match_per_word = True
	else:
		match_per_word = False

	if fuzziness:
		edges_searchterm0, matches0 = find_edges_searchterm_fuzziness(searchterm0, string_or_list, fuzziness)
		edges_searchterm1, matches1 = find_edges_searchterm_fuzziness(searchterm1, string_or_list, fuzziness)
	else:
		if isinstance(string_or_list, str):
			string0 = string_or_list
			edges_searchterm0 = find_edges_indices_searchterm(searchterm0, string0)
			edges_searchterm1 = find_edges_indices_searchterm(searchterm1, string0)

		elif isinstance(string_or_list, list):
			edges_searchterm0 = [index for index, word in enumerate(string_or_list) if word in searchterm0]
			edges_searchterm1 = [index for index, word in enumerate(string_or_list) if word in searchterm1]
		else:
			raise ValueError(f"{string_or_list} should be string or list but is {type(string_or_list)}")
	ranges_relevant_strings = find_ranges(edges_searchterm0, edges_searchterm1, distance)
	if ranges_relevant_strings is None:
		return []
	for relevant_range in ranges_relevant_strings:
		start_index = relevant_range[0]
		end_index = relevant_range[1]

		excerpt = create_excerpt_from_indices(string_or_list, start_index, end_index, searchterms + matches0 + matches1)
		areas_with_words_list.append(excerpt)
	return areas_with_words_list

def find_edges_searchterm_fuzziness(searchterm, string_or_list, fuzziness):
	if isinstance(string_or_list, str):
		match_per_word = False
	else:
		match_per_word = True
	__, nr_of_searchterms_found, indices = index_word_or_list({tuple(searchterm): 1},
															  string_or_list, fuzziness=fuzziness)
	if nr_of_searchterms_found == 0:
		return [], []
	edges_searchterm = [word_index_pair[1] + len(word_index_pair[0]) for word_index_pair in indices]
	matches = [word_index_pair[0] for word_index_pair in indices]
	return edges_searchterm, matches

def index_word_or_list(word_number_dictionary, contents, precise_index=False, fuzziness = False):
	"""Returns the index of a word_number_dictionary in a string (contents)"""
	output = ""
	is_there_output = True
	total_word_count = 0
	indices = []
	for searchterm, number in word_number_dictionary.items():
		correct_index = True
		if isinstance(searchterm, tuple):
			searchterm = list(searchterm)
			word_count, seperate_word_count_dictionary, local_indices = count_list(searchterm, contents, fuzziness = fuzziness)

		elif isinstance(searchterm, str):
			searchterm = searchterm.replace(' ',  '')
			word_count, local_indices = count_word(searchterm, contents, fuzziness = fuzziness)
		else:
			raise ValueError(f"{searchterm} should be str or tuple, but is {type(searchterm)}")

		indices += local_indices
		if precise_index:
			if word_count != number:
				correct_index = False
		elif not precise_index:
			if word_count < number:
				correct_index = False
		if correct_index:
			total_word_count += word_count
			if isinstance(searchterm, list):
				individual_word_counts = ""
				for key, value in seperate_word_count_dictionary.items():
					if value != 0:
						individual_word_counts = f"{individual_word_counts} {key}: {value},"
				individual_word_counts_searchterm = individual_word_counts[:-1]
				output = f"{output} {individual_word_counts_searchterm.title()}; total this category: {word_count}."


			elif isinstance(searchterm, str):
				output = f"{output} {searchterm.title()}:{word_count}."

		else:
			output = ""
			is_there_output = False

	if is_there_output == False:
		output = False
	return output, total_word_count, indices

def find_edges_indices_searchterm(searchterm, string):
	searchterm = str_to_list(searchterm)
	edges_searchterm = []
	for word in searchterm:
		matches = re.finditer(word, string)
		start_indices = [match.start() for match in matches]
		end_indices = [index+len(word) for index in start_indices]
		edges_word = start_indices + end_indices
		edges_searchterm += edges_word
	return edges_searchterm

def find_ranges(list1, list2, distance):
    """this function was written by primarily ChatGPT"""
    # Iterate through each number in both lists
    words = set()
    for num1 in list1:
        for num2 in list2:
            # Check if the absolute difference is within the specified distance
            if abs(num1 - num2) <= distance:
                # Add the corresponding words to a set
                words.add(num1)
                words.add(num2)

    # Create ranges for the numbers based on the words collected
    ranges = []
    current_range = None
    sorted_words = sorted(words)
    for num in sorted_words:
        if current_range is None:
            current_range = [num, num]
        elif num - current_range[1] <= distance:
            current_range[1] = num
        else:
            ranges.append((current_range[0], current_range[1]))
            current_range = [num, num]
    if current_range:
        ranges.append((current_range[0], current_range[1]))
    ranges.sort() #if it's empty, this makes it None
    return ranges


def create_excerpt_from_indices(string_or_list, start_index, end_index, searchterms, extra_words=10):
	"""creates excerpt that starts and ends three words before/after given indices"""
	required_spaces = extra_words + 1
	space_counter = 0
	if isinstance(string_or_list, str):
		while space_counter < required_spaces:
			start_index -= 1
			if start_index <= 0:
				start_index = 0
				break
			if string_or_list[start_index] == " ":
				space_counter += 1

		space_counter = 0
		while space_counter < required_spaces:
			end_index += 1
			if end_index >= len(string_or_list):
				end_index = len(string_or_list)
				break
			if string_or_list[end_index] == ' ':
				space_counter += 1
	else:
		start_index -= 3
		end_index += 4
	excerpt = string_or_list[start_index:end_index]
	excerpt = clean_excerpt(excerpt, searchterms)
	return excerpt

def count_word(searchterm, contents, fuzziness = False):
	"""finds how often a word appears in contents"""
	indices = []
	word = searchterm.upper()
	if isinstance(contents, str):
		contents = contents.upper()
	word = re.escape(word)
	matches = re.finditer(word, contents)
	indices = [(searchterm, match.start()) for match in matches] #this finds the full matches (+ their index) for every word
	absolute_word_count = len(indices)
	local_fuzziness = None
	if fuzziness:
		if isinstance(fuzziness, int):
			local_fuzziness = fuzziness
		elif searchterm in fuzziness.keys():
			local_fuzziness = fuzziness[searchterm]
		if local_fuzziness is not None:
			if isinstance(contents, str):
				for i in range(0, len(contents)):
					if check_closeness_indices(indices, i, word): #we don't want a word to count as match twice (e.g. once for ' apple pi' and once for 'apple pie')
						continue
					content_snippet = contents[i:i+len(word)]
					this_match_percentage = fuzz.ratio(content_snippet, word)
					if this_match_percentage >= local_fuzziness:
						if fuzz.ratio(contents[i+1:i+len(word)+1], word) > this_match_percentage:
							continue
						absolute_word_count += 1
						indices.append((content_snippet, i))
			elif isinstance(contents, list):
				for content_word in contents:
					if content_word == word:
						absolute_word_count +=1
						continue
					if fuzz.ratio(content_word, word) >= local_fuzziness:
						print("matched", content_word, "to", word)
						absolute_word_count += 1
						print(content_word)


	return absolute_word_count, indices

def count_list(searchterm, contents, fuzziness = False):
	"""finds how often a list of words appear in contents"""
	if isinstance(contents, str):
		contents = contents.upper()
	searchterm = str_to_list(searchterm)
	absolute_word_count = 0
	word_count_dictionary = {}
	indices = []
	for word in searchterm:
		single_word_count, indices_word = count_word(word, contents, fuzziness = fuzziness)
		indices += indices_word
		absolute_word_count += single_word_count
		word_count_dictionary[word] = single_word_count
	return absolute_word_count, word_count_dictionary, indices

def clean_excerpt(excerpt, searchterms):
	if isinstance(excerpt, list):
		excerpt = ' '.join(excerpt)
	excerpt = excerpt.lower()
	searchterms_to_replace = sorted(searchterms, key=len, reverse=True) #so that if for example both moeder and baarmoeder are in there, baarmoeder gets replaced first
	for term in searchterms_to_replace:
		excerpt =excerpt.replace(term.lower(), term.upper())
	# excerpt = re.sub(r'(?<!\n)\n(?!\n)', ' ', excerpt) #replacing single enters while keeping double enters
	excerpt = excerpt.replace("(cid:173)\n", " ")

	excerpt = excerpt.replace("\n", " ")
	excerpt.strip()
	excerpt = f"{excerpt}\n"
	return excerpt


def check_closeness_indices(indices, i, word):
	for word_index_pair in indices:
		word, index = unpack_word_index_pair(word_index_pair)
		if i-index < len(word):
			return True


def unpack_word_index_pair(word_index_pair):
	word = word_index_pair[0]
	index = word_index_pair[1]
	return word, index

def key_value_to_printable_and_URL(key, value, word, vergaderjaren_unspecified_dates = None):
	"""prints the key and the value. Key has URL in it."""
	if "vergaderjaar" not in key:
		date = key.replace(" ", ",").split(",")[0]
	else:
		date = ' '.join(key.replace(" ", ",").split(",")[:2])
	house = find_category(key)
	link, extra_output = find_url_in_title(key)
	if date == "date":
		identifier = key[-14:-4]
		date = f"vergaderjaar {vergaderjaren_unspecified_dates[identifier]}"
	if word:
		if isinstance(word, str):
			print(f"{date}, {house}. {word.title()}: {value}.\n {link}")
		elif isinstance(word, list):
			seperate_word_counts_str = ""
			if extra_output:
				return f"\n{date}, {house}\n Total: {value}. {extra_output}.", link
			else:
				return f"\n{date}, {house}\n Total: {value}.", link
	else:
		return f"{date}, {house}.", link

def find_category(key):
	"""finds a file is, depending on the house info (key) in the file name"""
	if "Tweede Kamer" in key:
		house_and_numbers = "Handelingen Tweede Kamer"
	elif "Eerste Kamer" in key:
		house_and_numbers = "Handelingen Eerste Kamer"
	elif "Verenigde Ve" in key:
		house_and_numbers = "Verenigde Vergadering"
	elif "Staten-Gener" in key:
		house_and_numbers = "Staten Generaal"
	elif "Kamerstuk" in key:
		key_list = key.split("amerstuk")
		house_and_numbers = f"kamerstuk{key_list[1][:-3]}"
	elif "register" in key.lower():
		house_and_numbers = "Register"
	elif "Kamervragen (Aanhangsel)" in key:
		house_and_numbers = "Kamervragen (Aanhangsel)"
	else:
		house_and_numbers = "TYPE UNKWONN, check find_category()"
	return house_and_numbers

def find_url_in_title(title):
	"""finds the URL in the title of the file, so it can print the URL"""
	for i in range(0,len(title)):
		if title[i:i+4] == " id ":
			start_identifier = i+4
		if "///" not in title:
			end_identifier = len(title)-4
			extra_output = False
		elif title[i:i+3] == "///":
			end_identifier = i
			if len(title) > i+3:
				extra_output = title[i+3:] #cuz then it's a list and has the individual wordcounts behind that
			else:
				extra_output = False
	identifier = title[start_identifier:end_identifier]
	link = f"https://zoek.officielebekendmakingen.nl/{identifier}"
	return link, extra_output

def print_data_row(datarow, print_excerpt):
	printable = f"{datarow['source']}"
	if datarow["url"] is not None:
		printable = f"{printable}\n{datarow['url']}"
	extra_prints = datarow['extra_prints']
	if extra_prints is not None:
		printable = f"{printable}\n{extra_prints}"
	if print_excerpt:
		if isinstance(datarow['excerpts'], list):
			for excerpt in datarow['excerpts']:
				printable = f"{printable}\n{excerpt}"
		elif isinstance(datarow["excerpts"], str):
			printable = f'{printable}\n{datarow["excerpts"]}'
	printable = f"{printable}\n"
	print(printable)
	return(f"{printable}\n")