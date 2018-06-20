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


def get_config_node(node, num_retries=10, num_retries_call=10, timeout=60):
    blockchain_version = u'0.0.0'
    sucessfull = True
    error_msg = None
    access_time = timeout
    config = {}
    start_total = timer()
    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)
        start = timer()
        blockchain_version = stm.get_blockchain_version()
        config = stm.get_config()
        stop = timer()
        access_time = stop - start        
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
    max_batch_size = None
    threading = False
    thread_num = 16

    last_block_id = 19273700
    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)
        blockchain = Blockchain(steem_instance=stm)

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
    total_duration = float("{0:.2f}".format(timer() - start_total))
    return {'sucessfull': sucessfull, 'node': node, 'error': error_msg,
            'total_duration': total_duration, 'count': block_count, 'access_time': None}

def benchmark_node_history(node, num_retries=10, num_retries_call=10, timeout=60, how_many_seconds=60):
    history_count = 0
    access_time = 0
    follow_time = 0
    sucessfull = False
    error_msg = None
    start_total = timer()

    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)
        account = Account("gtg", steem_instance=stm)

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

    total_duration = float("{0:.2f}".format(timer() - start_total))
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

    [author, permlink, voter] = resolve_authorpermvoter(authorpermvoter)
    authorperm = construct_authorperm(author, permlink)

    try:
        stm = Steem(node=node, num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout)

        start = timer()
        Vote(authorpermvoter, steem_instance=stm)
        stop = timer()
        vote_time = stop - start
        start = timer()
        Comment(authorperm, steem_instance=stm)
        stop = timer()
        comment_time = stop - start
        start = timer()
        Account(author, steem_instance=stm)
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
        df_sum = 0
        df_head_sum = timedelta(0)
        df_count = 0
        df_head_count = 0
        for i in range(0, 5):
            df = b_head.get_current_block_num() - b.get_current_block_num()
            df_head = addTzInfo(datetime.utcnow()) - ["timestamp"]
            time.sleep(3)
            df_sum += df
            df_count += 1
            df_head_sum += df_head
            df_head_count += 1            

        block_head_diff = df_head_sum.total_seconds() / df_head_count
        block_diff = df_sum / df_count
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
            'total_duration': total_duration, 'access_time': None, 'count': None, 'block_diff_head': block_diff, 'block_head_diff_sec': block_head_diff}

def run_config_benchmark(nodes, num_retries, num_retries_call, timeout, threading=True):
    results = []
    if threading and FUTURES_MODULE:
        pool = ThreadPoolExecutor(max_workers=len(nodes) + 1)
        futures = []
        for node in nodes:
            futures.append(pool.submit(get_config_node, node, num_retries, num_retries_call, timeout))
        try:
            results = [r.result() for r in as_completed(futures)]
        except KeyboardInterrupt:
            quit_thread = True
            print("benchmark aborted.")
    else:
        for node in nodes:
            print("Current node:", node)
            result = get_config_node(node, num_retries, num_retries_call, timeout)
            results.append(result)
    return results

def run_block_benchmark(working_node_list, num_retries, num_retries_call, timeout, how_many_seconds, threading=True):
    results_block = []
    if threading and FUTURES_MODULE:
        pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
        futures = []
        for node in working_node_list:
            futures.append(pool.submit(benchmark_node_blocks, node, num_retries, num_retries_call, timeout, how_many_seconds))
        try:
            results_block = [r.result() for r in as_completed(futures)]
        except KeyboardInterrupt:
            quit_thread = True
            print("benchmark aborted.")
    else:
        for node in working_node_list:
            print("Current node:", node)
            result = benchmark_node_blocks(node, num_retries, num_retries_call, timeout, how_many_seconds)
            results_block.append(result)
    return results_block

def run_hist_benchmark(working_node_list, num_retries, num_retries_call, timeout, how_many_seconds, threading=True):
    results_history = []
    if threading and FUTURES_MODULE:
        pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
        futures = []
        for node in working_node_list:
            futures.append(pool.submit(benchmark_node_history, node, num_retries, num_retries_call, timeout, how_many_seconds))
        try:
            results_history = [r.result() for r in as_completed(futures)]
        except KeyboardInterrupt:
            quit_thread = True
            print("benchmark aborted.")
    else:
        for node in working_node_list:
            print("Current node:", node)
            result = benchmark_node_history(node, num_retries, num_retries_call, timeout, how_many_seconds)
            results_history.append(result)
    return results_history

def run_call_benchmark(working_node_list, authorpermvoter, num_retries, num_retries_call, timeout, threading):
    results_call = []
    if threading and FUTURES_MODULE:
        pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
        futures = []
        for node in working_node_list:
            futures.append(pool.submit(benchmark_calls, node, authorpermvoter, num_retries, num_retries_call, timeout))
        try:
            results_call = [r.result() for r in as_completed(futures)]
        except KeyboardInterrupt:
            quit_thread = True
            print("benchmark aborted.")
    else:
        for node in working_node_list:
            print("Current node:", node)
            result = benchmark_calls(node, authorpermvoter, num_retries, num_retries_call, timeout)
            results_call.append(result)
    return results_call

def run_block_diff_benchmark(working_node_list, num_retries, num_retries_call, timeout, threading):
    results_call = []
    if threading and FUTURES_MODULE:
        pool = ThreadPoolExecutor(max_workers=len(working_node_list) + 1)
        futures = []
        for node in working_node_list:
            futures.append(pool.submit(benchmark_block_diff, node, num_retries, num_retries_call, timeout))
        try:
            results_call = [r.result() for r in as_completed(futures)]
        except KeyboardInterrupt:
            quit_thread = True
            print("benchmark aborted.")
    else:
        for node in working_node_list:
            print("Current node:", node)
            result = benchmark_block_diff(node, num_retries, num_retries_call, timeout)
            results_call.append(result)
    return results_call
