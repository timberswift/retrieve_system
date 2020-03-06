#coding='utf-8'
import requests


def query_api(api_key, query_key="工商银行", size=200):
    print("searching keyword '%s' in all documents ..." % query_key)
    try:
        size = int(size)
        assert size > 0
    except:
        size = 0
    result = {}
    try:
        result = requests.get('http://120.0.0.1:5000/query_sys&api_key='+api_key+'/word='+query_key+'&size='+str(size))
        result = eval(result.text)
    except Exception as e:
        print("ERROR: ", e)
    return result


if __name__ == "__main__":
    result = query_api(api_key="******", query_key="新冠肺炎", size=100)

