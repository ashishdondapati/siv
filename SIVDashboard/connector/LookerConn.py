#-*-coding:utf-8-*-

#https://github.com/looker-open-source/sdk-codegen/tree/master/python
#https://github.com/looker-open-source/sdk-codegen/blob/master/examples/python/run_look_with_filters.py

import json
import sys, os
from typing import cast, Dict, List, Union

import looker_sdk
from looker_sdk import models, error

TJson = List[Dict[str, Union[str, int, float, bool, None]]]

class LookerConn():
    sdk = None
    
    def __init__(self, path):
        self.sdk = looker_sdk.init31(path)        
        
    def get_look(self, id):
        look = None
        try:
            look = self.sdk.look(id)
        except Exception as e:
            print(e)
        else:
            print('success get info for %d' % id)

        return look
        
    def run_look(self, id):
        rt = None
        try:
            rt = self.sdk.run_look(id, 'json')
        except Exception as e:
            print(e)
            return None
        else:
            print('success get look data for %d' % id)

        return rt
        
    # change filter for existing look and run it.
    def run_look_with_filter(self, id, filters):
        query =  self.get_look(id).query
        print('%d look query is below!' % id)
        #print(query)
        
        request = self.create_query_request(query, filters)
        print(request)
        
        try:
            json_ = self.sdk.run_inline_query("json", request, cache=False)
        except error.SDKError:
            raise sdk_exceptions.RunInlineQueryError("Error running query")
        else:
            #json_resp = cast(TJson, json.loads(json_))
            json_resp = json_
        return json_resp
        
    def create_query_request(self, q, filters):
        return models.WriteQuery(
            model=q.model,
            view=q.view,
            fields=q.fields,
            pivots=q.pivots,
            fill_fields=q.fill_fields,
            filters=filters,
            sorts=q.sorts,
            limit=q.limit,
            column_limit=q.column_limit,
            total=q.total,
            row_total=q.row_total,
            subtotals=q.subtotals,
            dynamic_fields=q.dynamic_fields,
            query_timezone=q.query_timezone,
        )