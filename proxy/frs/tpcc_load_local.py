import json
import dejimautils
import tpccutils
import config
import uuid
from psycopg2.extras import DictCursor

class TPCCLoad_Local(object):
    def __init__(self):
        pass

    def on_get(self, req, resp):
        # get params
        params = req.params

        print("load start")

        # prepare db_conn and cursor
        global_xid = config.peer_name + '-' + str(uuid.uuid4())
        db_conn = config.connection_pool.getconn(key=global_xid)
        cur = db_conn.cursor(cursor_factory=DictCursor)

        # execution
        try:
            cur.execute(tpccutils.get_loadstmt_for_warehouse())
            cur.execute(tpccutils.get_loadstmt_for_item())
            for i in [1, 20001, 40001, 60001, 80001]:
                cur.execute(tpccutils.get_loadstmt_for_stock(i))
            cur.execute(tpccutils.get_loadstmt_for_district())
        except Exception as e:
            print(e)
            db_conn.rollback()
            config.connection_pool.putconn(db_conn)
            resp.text = "execution error"
            return

        db_conn.commit()

        cur.close()
        config.connection_pool.putconn(db_conn)

        resp.text = "success"
        return
