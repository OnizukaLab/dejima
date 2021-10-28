import json
import dejimautils
import tpccutils
import config
import uuid
from psycopg2.extras import DictCursor

class TPCCLoad_Customer(object):
    def __init__(self):
        pass

    def on_get(self, req, resp):
        # get params
        params = req.params

        # prepare other variables (dt_list, bt_list, all_peers)
        dt_list = [dt for dt in config.dejima_config_dict['dejima_table'].keys() if config.peer_name in config.dejima_config_dict['dejima_table'][dt]]
        bt_list = config.dejima_config_dict['base_table'][config.peer_name]
        all_peers = list(config.dejima_config_dict['peer_address'].keys())
        all_peers.remove(config.peer_name)

        print("load start")
        global_xid = config.peer_name + '-' + str(uuid.uuid4())
        config.tx_management_dict[global_xid] = {'child_peer_list': []}
        commit = True

        # prepare db_conn and cursor
        db_conn = config.connection_pool.getconn(key=global_xid)
        cur = db_conn.cursor(cursor_factory=DictCursor)

        # lock all dbs
        result = dejimautils.lock_request(['dummy'], all_peers, global_xid, config.dejima_config_dict)

        # execution
        try:
            d_id = int(config.peer_name.strip("Peer")) + 1
            c_stmt, h_stmt = tpccutils.get_loadstmt_for_customer_history(d_id)
            cur.execute(c_stmt)
            cur.execute(h_stmt)
            o_stmt, ol_stmt, no_stmt = tpccutils.get_loadstmt_for_orders_neworders_orderline(d_id)
            cur.execute(o_stmt)
            cur.execute(ol_stmt)
            cur.execute(no_stmt)
        except Exception as e:
            print(e)
            db_conn.rollback()
            del config.tx_management_dict[global_xid]
            config.connection_pool.putconn(db_conn)
            msg = "execution error"
            resp.text = msg
            return

        # propagation
        try:
            cur.execute("SELECT txid_current()")
            local_xid, *_ = cur.fetchone()
            prop_dict = {}
            for dt in dt_list:
                target_peers = list(config.dejima_config_dict['dejima_table'][dt])
                target_peers.remove(config.peer_name)
                if target_peers == []: continue

                for bt in bt_list:
                    cur.execute("SELECT {}_propagate_updates_to_{}()".format(bt, dt))
                cur.execute("SELECT public.{}_get_detected_update_data({})".format(dt, local_xid))
                delta, *_ = cur.fetchone()

                if delta == None: continue
                delta = json.loads(delta)

                prop_dict[dt] = {}
                prop_dict[dt]['peers'] = target_peers
                prop_dict[dt]['delta'] = delta

                config.tx_management_dict[global_xid]["child_peer_list"].extend(target_peers)
        except:
            db_conn.rollback()
            del config.tx_management_dict[global_xid]
            config.connection_pool.putconn(db_conn)
            msg = "birds error"
            resp.text = msg
            return
        
        result = "Ack"
        if prop_dict != {}:
            result = dejimautils.prop_request(prop_dict, global_xid, config.peer_name, config.dejima_config_dict)

        if result != "Ack":
            commit = False

        # termination
        target_list = config.tx_management_dict[global_xid]['child_peer_list']
        if commit:
            db_conn.commit()
            if target_list != []: dejimautils.termination_request(target_list, "commit", global_xid, config.dejima_config_dict)
        else:
            db_conn.rollback()
            if target_list != []: dejimautils.termination_request(target_list, "abort", global_xid, config.dejima_config_dict)

        # delete tx_management_dict entry and close cursor and put conn back
        del config.tx_management_dict[global_xid]
        cur.close()
        config.connection_pool.putconn(db_conn)
        dejimautils.release_lock_request(all_peers, global_xid, config.dejima_config_dict) 

        if commit:
            msg = "success"
            print("success")
        else:
            msg = "prop error"
        resp.text = msg
        return
