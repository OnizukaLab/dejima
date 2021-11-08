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
            
        # benchmark
        commit_num = 0
        abort_num = 0
        result_per_epoch = [{'commit': 0, 'abort': 0}]
        epoch = 0
        epoch_time = 100
        next_epoch_start_time = epoch_time
        start_time = time.time()
        print("benchmark start")
        random.seed()

        # frs & 2pl
        if METHOD == "frs" or METHOD == "2pl":
            if METHOD == "frs":
                doYCSB = doYCSB_frs
            else:
                doYCSB = doYCSB_2pl

            current_time = time.time() - start_time
            while (current_time < bench_time):
                current_time = time.time() - start_time
                # epoch update
                if current_time > next_epoch_start_time:
                    next_epoch_start_time += epoch_time
                    epoch += 1
                    result_per_epoch.append({'commit': 0, 'abort': 0})

                result = doYCSB()
                if result:
                    commit_num += 1
                    result_per_epoch[epoch]['commit'] += 1
                else:
                    result_per_epoch[epoch]['abort'] += 1
                    abort_num += 1

        # hybrid
        elif METHOD == "hybrid":
            current_method = "2pl"
            doYCSB = doYCSB_2pl
            check_time = 30
            check_interval = 300
            # determine first check timing
            next_check = random.randint(0,check_interval - check_time * 2)

            temp_commit = {'before': {'commit': 0, 'abort': 0}, 'after': {'commit': 0, 'abort': 0}}
            temp_changed_mode_flag = False

            current_time = time.time() - start_time
            while (current_time < bench_time):
                current_time = time.time() - start_time

                # epoch update
                if current_time > next_epoch_start_time:
                    next_epoch_start_time += epoch_time
                    epoch += 1
                    result_per_epoch.append({'commit': 0, 'abort': 0})
                # normal mode
                if current_time < next_check:
                    result = doYCSB()
                    if result:
                        commit_num += 1
                    else:
                        abort_num += 1

                # check mode
                # before
                elif current_time < next_check + check_time:
                    result = doYCSB()
                    if result:
                        temp_commit['before']['commit'] += 1
                        commit_num += 1
                    else:
                        temp_commit['before']['abort'] += 1
                        abort_num += 1
                # after
                elif current_time < next_check + check_time * 2:
                    # change method if this is first time
                    if not temp_changed_mode_flag:
                        temp_changed_mode_flag = True
                        if current_method == "2pl":
                            current_method = "frs"
                            doYCSB = doYCSB_frs
                        else:
                            current_method = "2pl"
                            doYCSB = doYCSB_2pl

                    result = doYCSB()
                    if result:
                        temp_commit['after']['commit'] += 1
                        commit_num += 1
                    else:
                        temp_commit['after']['abort'] += 1
                        abort_num += 1

                # return to normal, don't execute Tx
                else:
                    next_check += check_interval
                    if temp_commit['after']['commit'] < temp_commit['before']['commit']:
                        if current_method == "2pl":
                            current_method = "frs"
                            doYCSB = doYCSB_frs
                        else:
                            current_method = "2pl"
                            doYCSB = doYCSB_2pl
                    else:
                        if current_method == "2pl":
                            print("{} {}: FRS -> 2PL".format(config.peer_name, current_time))
                        else:
                            print("{} {}: S2PL -> FRS".format(config.peer_name, current_time))

                    temp_changed_mode_flag = False
                    temp_commit = {'before': {'commit': 0, 'abort': 0}, 'after': {'commit': 0, 'abort': 0}}
                    continue

                if result:
                    result_per_epoch[epoch]['commit'] += 1
                else:
                    result_per_epoch[epoch]['abort'] += 1

        # invalid method
        else:
            resp.text = "invalid method"
            return

        msg = " ".join([config.peer_name, str(commit_num), str(abort_num)])
        for result in result_per_epoch:
            msg += " " + str(result['commit'])
        for result in result_per_epoch:
            msg += " " + str(result['abort'])
        msg += "\n"

        print("benchmark finished")
        resp.text = msg
        return
