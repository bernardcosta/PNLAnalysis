QUERY_LAST_DATA = {
                  "query": {
                    "match_all": {}
                  },
                  "size": 1,
                  "sort": [
                    {
                      "date": {
                        "order": "desc"
                      }
                    }
                  ]
                }
