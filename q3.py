"""
Q1: Create index and load data
Please fill in the missing content in each function.
"""

import assignment4 as a4
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


def main():
    """
    The main function, do not change any code here
    """

    es = Elasticsearch()
    ic = IndicesClient(es)
    a4.create_wikipedia_index(ic)
    a4.load_data(es)

    print("The top ranked title:", search_and_rank(es))
    add_synonyms_to_index(ic)
    print("The top ranked title:", search_and_rank(es))
    print(how_does_rank_work())


def search_and_rank(es: Elasticsearch) -> str:
    """
    Based on the search in Q2, rank the documents by the terms "BC", "WA" and "AB"
    in the document body.
    Return the **title** of the top result.

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    str
        The title of the top ranked document
    """

    b1 = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "body": {
                            "query": "lake tour",
                            "operator": "or"
                        }
                    },
                    "match": {
                        "body": {
                            "query": "BC",
                            "boost": 2
                        }
                    },
                    "match": {
                        "body": {
                            "query": "AB",
                            "boost": 1
                        }
                    },
                    "match": {
                        "body": {
                            "query": "WA",
                            "boost": 1
                        }
                    }
                },
                "must_not": {
                    "match_phrase": {
                        "body": "Please improve this article if you can."
                    }
                }                
            }
        }
    }

    es.indices.refresh(index="wikipedia")
    res = es.search(index="wikipedia", body=b1)
    print("*** = ", res['hits']['total']['value'])
    return res['hits']['hits'][0]['_source']['title']


def add_synonyms_to_index(ic: IndicesClient) -> None:
    """
    Modify the index setting, add synonym mappings for "BC" => "British Columbia",
    "WA" => "Washington" and "AB" => "Alberta"

    Parameters
    ----------
    ic : IndicesClient
        The client for control index settings in Elasticsearch
    
    Returns
    -------
    None
    """

    request_body = {
        "settings":{
            "analysis":{
                "analyzer":{
                    "my_analyzer":{ 
                       "type":"custom",
                       "tokenizer":"standard",
                       "filter":[
                          "lowercase",
                          "my_stops",
                          "my_synonyms"
                       ]
                    }
                },
                "filter":{
                    "my_stops":{
                        "type":"stop",
                        "stopwords_path": "stopwords.txt"
                    },
                    "my_synonyms":{
                        "type": "synonym",
                        "synonyms": [
                            "BC => British Columbia",
                            "WA => Washington",
                            "AB => Alberta"
                        ]
                    }
                }
            }
        }

    }
    ic.close(index="wikipedia")
    ic.put_settings(index="wikipedia", body=request_body)
    ic.open(index="wikipedia")

def how_does_rank_work() -> str:
    """
    Please write the answer of the question:
    'how does rank work?' here, returning it as a str.

    Returns
    -------
    str, the answer
    """
    # Fill in the answer here

    ans = "In Q3.1, I added a new filter my_synonyms that maps BC, WA, and AB to British Columbia, Washington, and Alberta respectively, so that Elasticsearch will recognize the abbreviations BC, AB, and WA. For Q3.2, I added a boost parameter to the query with the weights specified by the TA (2 for BC, and 1 for AB, WA), so that when displaying the query result, documents containing BC will display on top, then AB, WA, and the rest."

    return ans


if __name__ == "__main__":
    main()
