import time
import config
import random
from frs.do_ycsbtx import doYCSB_frs
from two_pl.do_ycsbtx import doYCSB_2pl

class YCSB(object):
    def __init__(self):
        pass

    def on_get(self, req, resp):
        # get params
        params = req.params
        param_keys = ["bench_time", "method"]
        for key in param_keys:
            if not key in params.keys():
                msg = "Invalid parameters"
                resp.text = msg
                return
        bench_time = int(params['bench_time'])
        METHOD = params['method']
            
        commit_num = 0
        abort_num = 0

        start_time = time.time()

        # benchmark
        print("benchmark start")
        random.seed()
        while (time.time()-start_time < bench_time):
            # do YCSB
            if METHOD == "frs":
                result = doYCSB_frs()
            elif METHOD == "2pl":
                result = doYCSB_2pl()
            elif METHOD == "hybrid":
                # TODO: choose method
                result = doYCSB_frs()
            else:
                resp.text = "invalid method"
                return

            if result:
                commit_num += 1
            else:
                abort_num += 1

        msg = " ".join([config.peer_name, str(commit_num), str(abort_num), str(bench_time), str(commit_num/bench_time)]) + "\n"

        print("benchmark finished")
        resp.text = msg
        return
