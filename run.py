from main import main

#input list of with two or three searchterms that are list or str
words_near_list = [['baarmoeder', 'kind', 'baby', 'geboren', 'moeder', 'kinderen', 'gebooren', 'zuigeling'], ['emancipatie', 'slavin', 'slavinnen', 'slave', "lijfeigen", "negro" 'vrijverklaring', 'vrij verklaren', 'vrij verklaard', 'neger', 'zwarten']]
#input distance here if calling words_near_list
distance = 100
#input dates in format dd-mm-yyyy or yyyy-mm-dd here
start_date = "1814-00-00"
end_date = "1899-00-00"
# Input False (much faster) or a value below 100, for what percentage match it must be to be considered a close enough match
fuzziness = {'vrij verklaard': 93,  'geboren': 83, 'kinderen': 83, 'gebooren': 83, 'vrijverklaring': 83, 'vrij verklaren': 83, 'baarmoeder': 83, 'emancipatie': 80, 'slavinnen': 80, "zuigeling": 80, "lijfeigen":80, "negro":80}
#say True if you want to print an excerpt. If using index_word_count, excerpt will focus on first tuple in the key in index_word_count if that tuple is longer than 1 item
print_excerpt = True
#specifies the minimum length of the excerpt you want printed
length_excerpt = 500
#Do you just want one excerpt per file? If so, prints best excerpt. If not, prints every excerpt that either fits with a dictionary you specify here, or if you specify True, any excerpt that fits the same criteria as index_word_count

# when you want to save your output to txt, put the name of the txt file you want here
save_as_txt = "output.txt"
#input the directory that has the parliamentary documents
directory = "../NL parliamentary documents"

main(words_near_list, distance, start_date, end_date, save_as_txt, print_excerpt, length_excerpt, fuzziness, directory)
