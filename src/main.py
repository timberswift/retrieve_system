from elasticsearch import Elasticsearch
from tqdm import tqdm
from elasticsearch import helpers
import pandas as pd
import requests
import os
import json
from api import query_api
es = Elasticsearch()


class RetrieveSys(object):
    def __init__(self):
        self.search_index = "my_index_total"
        self.search_doc = "my_doc"
        self.database = []

    def delete_indices(self):
        if True and es.indices.exists(self.search_index):  # 确认删除再改为True
            print("删除之前存在的index")
            es.indices.delete(index=self.search_index)

    def create_index(self):
        # index settings
        settings = \
            {
                "mappings": {
                    self.search_doc: {
                        "properties": {
                            "my_id": {"type": "integer"},
                            # "my_word": {"type": "text", "analyzer": "ik_smart", "search_analyzer": "ik_smart"}
                            # "my_word": {"type": "text", "analyzer": "no_analyzed", "search_analyzer": "no_analyzed"}
                            # "my_word": {"type": "text", "analyzer": "ik_smart", "search_analyzer": "no_analyzed"}
                            # "my_word": {"type": "text", "analyzer": "no_analyzed", "search_analyzer": "ik_smart"}
                            # "my_word": {"type": "text", "analyzer": "ik_smart", "search_analyzer": "ik_max_word"}
                            # "my_word": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_max_word"}
                            # "my_word": {"type": "text", "search_analyzer": "not_analyzed"}
                            "my_word": {"type": "string", "index": "not_analyzed"}
                            # "title": {}
                        }
                    }
                },
                'similarity': {
                    'default': {
                        'type': 'BM25'
                    }
                }
            }
        # create index
        es.indices.create(index=self.search_index, ignore=400, body=settings)
        print("创建index成功！")

    def main_create_index(self):
        # 调用后创建index
        self.delete_indices()
        self.create_index()

    def insert_data(self, one_bulk):
        # 插入数据
        body = []
        body_count = 0  # 记录body里面有多少个.
        # 最后一个bulk可能没满one_bulk,但也要插入

        print("共需要插入%d条..." % len(self.database))
        p_bar = tqdm(total=len(self.database))

        for id, word in self.database:
            data = {"my_id": id, "my_word": word}
            every_body = \
                {
                    "_index": self.search_index,
                    "_type": self.search_doc,
                    "_source": data
                }

            if body_count < one_bulk:
                body.append(every_body)
                body_count += 1
            else:
                helpers.bulk(es, body)  # 还是要用bulk啊，不然太慢了
                p_bar.update(one_bulk)
                body_count = 0
                body = []
                body.append(every_body)
                body_count += 1

        if len(body) > 0:
            # 如果body里面还有，则再插入一次（最后非整块的）
            helpers.bulk(es, body)
            # pbar.update(len(body))

        p_bar.close()
        # res = es.index(index=my_index,doc_type=my_doc,id=my_key_id,body=data1)  #一条插入
        print("插入数据完成!")

    def main_insert(self):
        # 调用后插入数据
        self.database = read_data()
        # self.database = getAllWords()
        self.insert_data(one_bulk=5000)

    def keyword_search(self, keyword='氨基酸', size=0):
        # 根据keywords1来查找，倒排索引
        my_search = \
            {
                "query": {
                    "match": {
                        "my_word": {
                            "query": keyword,
                            # "operator": "and",
                            "minimum_should_match": 1
                        }
                    }
                }
            }

        print("Searching ... ")
        # Type 1: 直接查询
        es_result = es.search(index=self.search_index, scroll='2m', body=my_search, size=9999)
        total = es_result["hits"]["total"]['value']
        # print("共查询到%d条数据" % total)
        es_result = es_result['hits']['hits']

        search_type = "large"
        if not size:
            size = total

        if size <= 10000 and total > 20000:
            # Type 1: 直接查询 全部都是精确
            search_type = "small"
            pass
        else:
            # Type 2: scroll 查询
            data = es.search(
                index=self.search_index,
                doc_type=self.search_doc,
                scroll='1s',
                body=my_search,
                size=10000
            )
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])
            es_result = []
            while scroll_size > 0:
                es_result += data['hits']['hits']
                data = es.scroll(scroll_id=sid, scroll='1s')
                sid = data['_scroll_id']
                scroll_size = len(data['hits']['hits'])

        # # es_result = es_result[: -int(len(es_result)*0.5)]

        # repeat_table = []
        # new_result = []
        # for it in es_result:
        #     if it['_source']['my_word'] not in repeat_table:
        #         new_result.append(it)
        #         repeat_table.append(it['_source']['my_word'])
        #
        # es_result = new_result
        search_res = []
        total_score = 0
        new_total = 0
        try:
            # set accuracy 1
            top_score = es_result[0]['_score']
            es_result = [item for item in es_result if item['_score'] >= top_score*0.3]
            new_total = len(es_result)
            if size and new_total and size / new_total < 0.35:
                search_type = "small"

            scores = [item['_score'] for item in es_result]
            for i in scores:
                total_score += i
            avg_score = total_score/len(scores)
            print("avg score: ", avg_score)
            for i, item in enumerate(es_result):
                tmp = item['_source']
                ans_item = {"accuracy": "", "score": item['_score'], "id": i, "text": tmp['my_word']}

                # set accuracy 2
                if search_type == 'large':
                    if ans_item['score'] >= avg_score*1.2:
                        ans_item['accuracy'] = '精确'
                    elif ans_item['score'] <= avg_score*0.7:
                        ans_item['accuracy'] = '模糊'
                    else:
                        ans_item['accuracy'] = '一般'
                else:
                    ans_item['accuracy'] = '精确'
                search_res.append(ans_item)

        except:
            pass

        if size:
            search_res = search_res[:size]
        print("共查询到%d条数据" % new_total)
        print("查询结果显示前%d条数据" % len(search_res))
        return search_res, new_total


def read_data():
    import codecs
    data_path = ""
    for dir, _ ,filename in os.walk("../database", topdown=False):
        for name in filename:
            if name == "ce_news_content.txt":
                data_path = os.path.join(dir, name)
    news = pd.read_csv(data_path, sep='\t', error_bad_lines=False)
    data = list(news['text'])
    data = [(i, d) for i, d in enumerate(data)]
    return data

def create_index():
    try:  
        rs = RetrieveSys()
        rs.main_create_index()
        rs.main_insert()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    result = query_api(api_key="******",query_key="新冠肺炎", size=100)

