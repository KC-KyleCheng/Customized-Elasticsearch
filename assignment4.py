from typing import Tuple

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from pyquery import PyQuery
import tarfile, os
import sys


def load_data(es: Elasticsearch) -> None:
    """
    This function loads data from the tarball "wiki-small.tar.gz" 
    to the Elasticsearch cluster

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    None
    """
    i = 1
    tar = tarfile.open("wiki-small.tar.gz")
    for member in tar.getmembers():
        f = tar.extractfile(member)
        if (f != None):
            content = f.read()
            d1 = {
                "title": parse_html(content)[0],
                "body": parse_html(content)[1],
            }            
            es.index(index="wikipedia", id=i, body=d1)
            i += 1
    tar.close()


def parse_html(html: str) -> Tuple[str, str]:
    """
    This function parses the html, strips the tags an return
    the title and the body of the html file.

    Parameters
    ----------
    html : str
        The HTML text

    Returns
    -------
    Tuple[str, str]
        A tuple of (title, body)
    """

    doc = PyQuery(html)
    for title in doc("title").items():
        title_str = title.text()
    for body in doc("body").items():
        body_str = body.text()
    return title_str, body_str
    

def create_wikipedia_index(ic: IndicesClient) -> None:
    """
    Add an index to Elasticsearch called 'wikipedia'

    Parameters
    ----------
    ic : IndicesClient
        The client to control Elasticsearch index settings

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
                          "my_stops"
                       ]
                    }
                },
                "filter":{
                    "my_stops":{
                        "type":"stop",
                        "stopwords_path": "stopwords.txt"
                    }
                }
            }
        }

    }
    
    ic.create(index="wikipedia", body=request_body)
