from flask import Flask, url_for, jsonify, Response
from main import *
import json

app = Flask(__name__)


@app.route('/')
def api_root():
    welcome = {
        "welcome": "Public opinion news data Retrieve-System API",
        "API":
            {
                "web": "http://host:port/query_api/key=<query_key>&size=<result_size>",
                "python": "query_result = query_api(api_host='127.0.0.1:5000', query_key='新冠肺炎', size=100)"
            },
        "author": "Copyright©ICRC 2020/3/5  973149077@qq.com"
    }
    return Response(json.dumps(welcome, ensure_ascii=False, indent=4), mimetype='application/json')


app.config['JSON_AS_ASCII'] = False


@app.route('/query_sys&api_key=<api_key>/word=<keyword>&size=<size>')
def api_query(api_key, keyword, size):
    if api_key == "********":
        try:
            size = int(size)
            assert size > 0
        except:
            size = 0
        try:
            search_res, total = RetrieveSys().keyword_search(keyword=str(keyword), size=int(size))
            print(search_res)
            result = {"info": "共查询到%d条数据, 当前显示前%d条" % (total, len(search_res)), "result": search_res}
            return Response(json.dumps(result, ensure_ascii=False, indent=2), mimetype='application/json')
        except Exception as e:
            return str(e)
    else:
        return "Invalid API key."
    # return jsonify(result)


if __name__ == '__main__':
    app.run(port=5000)
