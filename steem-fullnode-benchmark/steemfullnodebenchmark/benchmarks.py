from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import sys
from datetime import datetime, timedelta, date
import time
import io
import json
from timeit import default_timer as timer
import logging
from beem.blockchain import Blockchain
from beem.account import Account
from beem.block import Block
from beem.steem import Steem
from beem.utils import parse_time, formatTimedelta, construct_authorperm, resolve_authorperm, resolve_authorpermvoter, construct_authorpermvoter, formatTimeString, addTzInfo
from beem.comment import Comment
from beem.nodelist import NodeList
from beem.witness import WitnessesRankedByVote
from beem.vote import Vote
from beemapi.exceptions import NumRetriesReached
FUTURES_MODULE = None
if not FUTURES_MODULE:
    try:
        from concurrent.futures import ThreadPoolExecutor, wait, as_completed
        FUTURES_MODULE = "futures"
    except ImportError:
        FUTURES_MODULE = None
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR)
quit_thread = False


def get_config_node(node, num_retries=10, num_retries_call=10, timeout=60, how_many_seconds=30):
    blockchain_version = u'0.0.0'
    sucessfull = True
    error_msg = None
    access_time = timeout
    config = {}
    start_total = timer()
    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)
        blockchain_version = stm.get_blockchain_version()
        start = timer()
        config = stm.get_config(use_stored_data=False)
        stop = timer()
        access_time = stop - start
        config_count = 0
        
    except NumRetriesReached:
        error_msg = 'NumRetriesReached'
        sucessfull = False
    except KeyboardInterrupt:
        error_msg = 'KeyboardInterrupt'
        # quit = True
    except Exception as e:
        error_msg = str(e)
    total_duration = float("{0:.2f}".format(timer() - start_total))
    access_time = float("{0:.2f}".format(access_time))
    ret = {'sucessfull': sucessfull, 'node': node, 'error': error_msg,
                    'total_duration': total_duration, 'count': None,
                    'access_time': access_time, 'version': blockchain_version, 'config': config}
    return ret

def benchmark_node_blocks(node, num_retries=10, num_retries_call=10, timeout=60, how_many_seconds=30):
    block_count = 0
    sucessfull = False
    error_msg = None
    start_total = timer()
    start_time = timer()
    max_batch_size = None
    threading = False
    thread_num = 16

    last_block_id = 19273700
    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)
        blockchain = Blockchain(steem_instance=stm)
        
        last_block_id = int(blockchain.get_current_block_num() * 0.75)

        last_block = Block(last_block_id, steem_instance=stm)

        total_transaction = 0

        start_time = timer()
        for entry in blockchain.blocks(start=last_block_id, max_batch_size=max_batch_size, threading=threading, thread_num=thread_num):
            block_no = entry.identifier
            block_count += 1
            if not sucessfull:
                sucessfull = True
            if "block" in entry:
                trxs = entry["block"]["transactions"]
            else:
                trxs = entry["transactions"]

            for tx in trxs:
                for op in tx["operations"]:
                    total_transaction += 1
            if "block" in entry:
                block_time = (entry["block"]["timestamp"])
            else:
                block_time = (entry["timestamp"])

            if timer() - start_time > how_many_seconds or quit_thread:
                break
    except NumRetriesReached:
        error_msg = 'NumRetriesReached'
        sucessfull = False
    except KeyboardInterrupt:
        error_msg = 'KeyboardInterrupt'
        # quit = True
    except Exception as e:
        error_msg = str(e)
        sucessfull = False
    total_duration = float("{0:.2f}".format(timer() - start_time))
    return {'sucessfull': sucessfull, 'node': node, 'error': error_msg,
            'total_duration': total_duration, 'count': block_count, 'access_time': None}

def benchmark_node_history(node, num_retries=10, num_retries_call=10, timeout=60, how_many_seconds=60, account_name="gtg"):
    history_count = 0
    access_time = 0
    follow_time = 0
    sucessfull = False
    error_msg = None
    start_total = timer()
    start_time = timer()
    
    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)
        account = Account(account_name, steem_instance=stm)
        start_time = timer()
        for acc_op in account.history_reverse(batch_size=100):
            history_count += 1
            if not sucessfull:
                sucessfull = True
            if timer() - start_time > how_many_seconds or quit_thread:
                break
    except NumRetriesReached:
        error_msg = 'NumRetriesReached'
        sucessfull = False
    except KeyboardInterrupt:
        error_msg = 'KeyboardInterrupt'
        sucessfull = False
        # quit = True
    except Exception as e:
        error_msg = str(e)
        sucessfull = False

    total_duration = float("{0:.2f}".format(timer() - start_time))
    return {'sucessfull': sucessfull, 'node': node, 'error': error_msg,
            'total_duration': total_duration, 'count': history_count, 'access_time': None}


def benchmark_calls(node, authorpermvoter, num_retries=10, num_retries_call=10, timeout=60):
    block_count = 0
    history_count = 0
    access_time = timeout
    follow_time = 0
    sucessfull = False
    error_msg = None
    start_total = timer()
    account_name = "gtg"

    [author, permlink, voter] = resolve_authorpermvoter(authorpermvoter)
    authorperm = construct_authorperm(author, permlink)

    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)

        start = timer()
        Vote(authorpermvoter, steem_instance=stm)
        stop = timer()
        vote_time = stop - start
        start = timer()
        c = Comment(authorperm, steem_instance=stm)
        if c.title == '':
            raise AssertionError("title must not be empty!")
        stop = timer()
        comment_time = stop - start
        start = timer()
        acc = Account(author, steem_instance=stm)
        # if acc.json()["json_metadata"] == '':
        #    raise AssertionError("json_metadata must not be empty!")
        stop = timer()
        account_time = stop - start
        sucessfull = True
        access_time = (vote_time + comment_time + account_time) / 3.0
    except NumRetriesReached:
        error_msg = 'NumRetriesReached'
        sucessfull = False
    except KeyboardInterrupt:
        error_msg = 'KeyboardInterrupt'
        # quit = True
    except Exception as e:
        error_msg = str(e)
        sucessfull = False
    total_duration = float("{0:.2f}".format(timer() - start_total))
    access_time = float("{0:.3f}".format(access_time))
    return {'sucessfull': sucessfull, 'node': node, 'error': error_msg,
            'total_duration': total_duration, 'access_time': access_time, 'count': None}

def benchmark_block_diff(node, num_retries=10, num_retries_call=10, timeout=60):
    block_count = 0
    history_count = 0
    access_time = timeout
    follow_time = 0
    sucessfull = False
    error_msg = None
    start_total = timer()
    block_diff = 0
    block_head_diff = 0
    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)

        b = Blockchain(steem_instance=stm)
        b_head = Blockchain(mode="head", steem_instance=stm)
        dhi_max = 0
        head_max = timedelta(0)
        for i in range(0, 5):
            utcnow = addTzInfo(datetime.utcnow())
            df_head = addTzInfo(datetime.utcnow()) - b_head.get_current_block()["timestamp"]
            diff_value = b_head.get_current_block_num() - b.get_current_block_num()
            if diff_value > dhi_max:
                dhi_max = diff_value
            if df_head > head_max:
                head_max = df_head
            
            time.sleep(3)

        block_head_diff = head_max.total_seconds()
        block_diff = dhi_max
        sucessfull = True
    except NumRetriesReached:
        error_msg = 'NumRetriesReached'
        sucessfull = False
    except KeyboardInterrupt:
        error_msg = 'KeyboardInterrupt'
        # quit = True
    except Exception as e:
        error_msg = str(e)
        sucessfull = False
    total_duration = float("{0:.2f}".format(timer() - start_total))
    block_diff = float("{0:.2f}".format(block_diff))
    block_head_diff = float("{0:.2f}".format(block_head_diff))
    return {'sucessfull': sucessfull, 'node': node, 'error': error_msg,
            'total_duration': total_duration, 'access_time': None, 'count': None, 'diff_head_irreversible': block_diff, 'head_delay': block_head_diff}


class Benchmarks(object):
    def __init__(
        self,
        num_retries=10,
        num_retries_call=10,
        timeout=60
    ):
        self.num_retries = num_retries
        self.num_retries_call = num_retries_call
        self.timeout = timeout

    def run_config_benchmark(self, nodes, how_many_seconds, threading=True):
        results = []
        if threading and FUTURES_MODULE:
            pool = ThreadPoolExecutor(max_workers=len(nodes) + 1)
            futures = []
            for node in nodes:
                futures.append(pool.submit(get_config_node, node, self.num_retries, self.num_retries_call, self.timeout, how_many_seconds))
            try:
                results = [r.result() for r in as_completed(futures)]
            except KeyboardInterrupt:
                quit_thread = True
                print("benchmark aborted.")
        else:
            for node in nodes:
                print("Current node:", node)
                result = self.get_config_node(node, self.num_retries, self.num_retries_call, self.timeout, how_many_seconds)
                results.append(result)
        return results

    def run_block_benchmark(self, working_node_list, how_many_seconds, threading=True):
        results_block = []
        if threading and FUTURES_MODULE:
            pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
            futures = []
            for node in working_node_list:
                futures.append(pool.submit(benchmark_node_blocks, node, self.num_retries, self.num_retries_call, self.timeout, how_many_seconds))
            try:
                results_block = [r.result() for r in as_completed(futures)]
            except KeyboardInterrupt:
                quit_thread = True
                print("benchmark aborted.")
        else:
            for node in working_node_list:
                print("Current node:", node)
                result = benchmark_node_blocks(node, self.num_retries, self.num_retries_call, self.timeout, how_many_seconds)
                results_block.append(result)
        return results_block
    
    def run_hist_benchmark(self, working_node_list, how_many_seconds, threading=True, account_name="gtg"):
        results_history = []
        if threading and FUTURES_MODULE:
            pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
            futures = []
            for node in working_node_list:
                futures.append(pool.submit(benchmark_node_history, node, self.num_retries, self.num_retries_call, self.timeout, how_many_seconds, account_name))
            try:
                results_history = [r.result() for r in as_completed(futures)]
            except KeyboardInterrupt:
                quit_thread = True
                print("benchmark aborted.")
        else:
            for node in working_node_list:
                print("Current node:", node)
                result = benchmark_node_history(node, self.num_retries, self.num_retries_call, self.timeout, how_many_seconds, account_name)
                results_history.append(result)
        return results_history
    
    def run_call_benchmark(self, working_node_list, authorpermvoter, threading):
        results_call = []
        if threading and FUTURES_MODULE:
            pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
            futures = []
            for node in working_node_list:
                futures.append(pool.submit(benchmark_calls, node, authorpermvoter, self.num_retries, self.num_retries_call, self.timeout))
            try:
                results_call = [r.result() for r in as_completed(futures)]
            except KeyboardInterrupt:
                quit_thread = True
                print("benchmark aborted.")
        else:
            for node in working_node_list:
                print("Current node:", node)
                result = benchmark_calls(node, authorpermvoter, self.num_retries, self.num_retries_call, self.timeout)
                results_call.append(result)
        return results_call
    
    def run_block_diff_benchmark(self, working_node_list, threading):
        results_call = []
        if threading and FUTURES_MODULE:
            pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
            futures = []
            for node in working_node_list:
                futures.append(pool.submit(benchmark_block_diff, node, self.num_retries, self.num_retries_call, self.timeout))
            try:
                results_call = [r.result() for r in as_completed(futures)]
            except KeyboardInterrupt:
                quit_thread = True
                print("benchmark aborted.")
        else:
            for node in working_node_list:
                print("Current node:", node)
                result = benchmark_block_diff(node, self.num_retries, self.num_retries_call, self.timeout)
                results_call.append(result)
        return results_call
