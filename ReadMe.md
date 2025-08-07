# Parliament query near
This repository contains the Python code to execute a query on Dutch parliamentary documents that retrieves files where certain strings, or lists of possible strings, are near other strings or lists of possible strings. For example, the code is currently set to execute a query in which words related to slavery are within 100 characters of words related to motherhood or childbirth. This code was used in a research project about Dutch parliamentary discussions about whether or not to free children born to enslaved mothers. 

The code also allows you to work with fuziness; depending on your value for fuzziness, words don't have to match exactly. This is intended to deal with OCR issues, but can also be used in other ways. For instance, the code is currently set to allow a fuziness of 93 for "vrij verklaard", this means that something like "vrjj verklaard" will also work.

I report the occasional use of ChatGPT for aid in writing this code.

## Getting started
### Prerequisites


```
pip install pandas
```


```
pip install rapidfuzz
```


```
pip install re
```

### Installation
1. Download dataset from http://doi.org/10.5281/zenodo.16728400
2. Clone the repo


```
git clone https://github.com/Ida-L-Vos/parliament_near_query
```


## Usage
To run the query used for the research project for which this code was written, simply run the python file called "run.py".

To create your own query, open and edit the file called run.py. Further instruction for how to create your own query can be found in that file.

## Contact
Ida Vos - vos@eshcc.eur.nl

Project - https://github.com/Ida-L-Vos/parliament_near_query
