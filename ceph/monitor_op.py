#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
import re
import json
import subprocess
import traceback
import logging
descriptors = list()
local_path = "/var/run/ceph/"
log_path = "/var/log/ceph-monitor"
result_asok = {}

NAME_PREFIX = 'osd_'
num = 0

# log settings
logging.basicConfig(filename = "/var/log/monitor_op.log", level = logging.CRITICAL , filemode = "a", format = "%(asctime)s - %(thread)d - %(levelname)s - %(filename)s[line:%(lineno)d]: %(message)s")
log = logging.getLogger("root")

def run_shell(cmd, timeout = 4):
    p = subprocess.Popen(
        args = cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        shell = True
    )
    #must run shell with timeout
    # define deadline
    deadline = time.time() + timeout
    poll_seconds = 0.25
    while time.time() < deadline and p.poll() == None:
        time.sleep(poll_seconds)
    # get result
    ret = p.poll()
    if ret:
        # ret != 0
        return None
    if ret == None:
        # timeout
        try:
            p.terminate()
        except:
            pass
        return None
    res = ''
    while True:
        buff = p.stdout.readline()
        res = res + buff
        if buff == '':
            break;
    return res.rstrip()

def get_all_file(floder_path):
    file_list = []
    if floder_path is None:
        raise Exception("floder_path is None")
    for dirpath, dirnames, filenames in os.walk(floder_path):
        for name in filenames:
            file_list.append(dirpath + '\\' + name)
    return file_list
    
def get_local_osds():
    file_list = get_all_file(local_path)
    osd_list=[]
    for f in file_list:
        m = re.search("ceph-osd\.\d+\.asok",f)
        if m:
            id = f.split('.')[-2]
            osd_list.append(id)
    return osd_list

def get_last_val(filename):
    context = []
    if os.path.exists(filename):
        output = open(filename,'r')
        while True:
            line = output.readline()
            if not line:
                break
            context.append(line)
        output.close()
    return context

def update_val(id):
    global result_asok
    global local_path
    file_path = local_path + "ceph-osd."+id+".asok"
    result_asok[id] = json.loads(run_shell('ceph --admin-daemon {asok} perf dump'.format(asok=file_path)))
    return result_asok[id]
def set_current_val(filename,vals):
    
    if type(vals) != list:
        return -1
    output = open(filename,'w')
    for val in vals:
        output.write(str(val))
        output.write('\n')
    output.close()
    return 0
    
def get_oplatency_journal(name):
    
    try:
        id = name.split('_')[-1]
        result = update_val(id)
        if "filestore" in result:
            if "journal_latency" in result["filestore"]:
                current_sum = result["filestore"]["journal_latency"]["sum"]
                current_ops = result["filestore"]["journal_latency"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/journal_latency'+id)
                
                #cal 
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret = set_current_val('/dev/shm/journal_latency'+id)
                if ret != 0:
                    return 0
                if len(last_context) != 2 :
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(current_ops) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0
        
def get_oplatency_avgoplat(name):
    
    try:
        id = name.split('_')[-1]
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "osd" in result:
            if "op_latency" in result["osd"] and "subop_latency" in result["osd"]:
                current_op_sum = result["osd"]["op_latency"]["sum"]
                current_op_ops = result["osd"]["op_latency"]["avgcount"]
                current_subop_sum = result["osd"]["subop_latency"]["sum"]
                current_subop_ops = result["osd"]["subop_latency"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/op_latency'+id)
                
                #cal 
                current_sum = float(current_op_sum) + float(current_subop_sum)
                current_ops = int(current_op_ops) + int(current_subop_ops)
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret = set_current_val('/dev/shm/op_latency'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2:
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(current_ops) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_oplatency_opw(name):
    
    try:
        id = name.split('_')[-1]
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "op_w_latency" in result["osd"] and "subop_latency" in result["osd"]:
                current_op_sum = result["osd"]["op_w_latency"]["sum"]
                current_op_ops = result["osd"]["op_w_latency"]["avgcount"]
                current_subop_sum = result["osd"]["subop_latency"]["sum"]
                current_subop_ops = result["osd"]["subop_latency"]["avgcount"]
                last_context = []
                last_context = []
                last_context = get_last_val('/dev/shm/op_w_latency'+id)
                
                
                #cal 
                current_sum = float(current_op_sum) + float(current_subop_sum)
                current_ops = int(current_op_ops) + int(current_subop_ops)
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret = set_current_val('/dev/shm/op_w_latency'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2:
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(current_ops) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_oplatency_opr(name):
    
    try:
        id = name.split('_')[-1]
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "op_r_latency" in result["osd"]:
                current_sum = result["osd"]["op_r_latency"]["sum"]
                current_ops = result["osd"]["op_r_latency"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/op_r_latency'+id)
                
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret = set_current_val('/dev/shm/op_r_latency'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2:
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(last_sum) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_iops(name):
    
    try:
        id = name.split('_')[-1]
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "osd" in result:
            if "op_w" in result["osd"] and "subop" in result["osd"]:
                current_ops_w = result["osd"]["op_w"]
                current_subop_w = result["osd"]["subop"]
                last_context = []
                
                last_context = get_last_val('/dev/shm/iops'+id)
                #cal 
                current_ops = 0
                if "op_r" in result["osd"]:
                    current_ops_r = result["osd"]["op_r"]
                    current_ops = int(ops_w_end) + int(ops_subop_end) + int(ops_r_end)
                else:
                    current_ops = int(ops_w_end) + int(ops_subop_end)
                current_context = []
                current_context.append(str(current_ops))
                ret = set_current_val('/dev/shm/iops'+id,current_context)
                if len(last_context) != 1:
                    return 0
                last_ops = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    sum_ops = int(current_ops) - int(last_ops)
                    return sum_ops 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_oplatency_apply(name):
    
    try:
        id = name.split('_')[-1]
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "filestore" in result:
            if "apply_latency" in result["filestore"]:
                current_sum = result["filestore"]["apply_latency"]["sum"]
                current_ops = result["filestore"]["apply_latency"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/apply_latency'+id)
                
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret =set_current_val('/dev/shm/apply_latency'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2 :
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(currrent_ops) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0
def get_oplatency_commitcycle(name):
    
    try:
        id = name.split('_')[-1]
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "filestore" in result:
            if "commitcycle_latency" in result["filestore"]:
                current_sum = result["filestore"]["commitcycle_latency"]["sum"]
                current_ops = result["filestore"]["commitcycle_latency"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/commitcycle_latency'+id)
                
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret =set_current_val('/dev/shm/commitcycle_latency'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2 :
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(currrent_sum) - float(last_sum)
                    lat_ops = int(current_ops) - int(current_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_queue_transaction(name):
    
    try:
        id = name.split('_')[-1]
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "filestore" in result:
            if "queue_transaction_latency_avg" in result["filestore"]:
                current_sum = result["filestore"]["queue_transaction_latency_avg"]["sum"]
                current_ops = result["filestore"]["queue_transaction_latency_avg"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/queue_transaction_latency_avg'+id)
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret =set_current_val('/dev/shm/queue_transaction_latency_avg'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2 :
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(current_ops) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_oplatency_subopw(name):
    
    try:
        id = name.split('_')[-1]
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "subop_w_latency" in result["osd"]:
                current_sum = result["osd"]["subop_w_latency"]["sum"]
                current_ops = result["osd"]["subop_w_latency"]["avgcount"]
                last_context = []
                last_context = get_last_val('/dev/shm/subop_w_latency'+id)
                current_context = []
                current_context.append(current_sum)
                current_context.append(current_ops)
                ret = set_current_val('/dev/shm/subop_w_latency'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 2:
                    return 0
                last_ops = last_context[1].strip('\n')
                last_sum = last_context[0].strip('\n')
                if int(current_ops) == int(last_ops):
                    return 0
                else:
                    lat_sum = float(current_sum) - float(last_sum)
                    lat_ops = int(current_ops) - int(last_ops)
                    lat = (lat_sum * 1.0)/lat_ops

                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_bytesinct(name):
    
    try:
        id = name.split('_')[-1]
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "op_w_in_bytes" in result["osd"] and "subop_in_bytes" in result["osd"]:
                current_op_w_bytes = result["osd"]["op_w_in_bytes"]
                current_subop_bytes = result["osd"]["subop_in_bytes"]
                last_context = []
                last_context = get_last_val('/dev/shm/bytesinct'+id)
                
                #cal 
                current_bytes = int(current_op_w_bytes) + int(current_subop_bytes)
                current_context = []
                current_context.append(current_bytes)
                ret = set_current_val('/dev/shm/bytesinct'+id,current_context)
                if ret!= 0:
                    return 0
                if len(last_context) != 1:
                    return 0
                last_bytes = last_context[0].strip('\n')
                if int(current_bytes) == int(last_bytes):
                    return 0
                else:
                    r = int(current_bytes) - int(last_bytes)
                    return r
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0

def get_bytesoutct(name):
    
    try:
        id = name.split('_')[-1]
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "op_r_out_bytes" in result["osd"]:
                current_bytes = result["osd"]["op_r_out_bytes"]
                last_context = []
                last_context = get_last_val('/dev/shm/bytesoutct'+id)
                
                current_context = []
                current_context.append(current_bytes)
                ret = set_current_val('/dev/shm/bytesoutct'+id,current_context)
                if ret != 0:
                    return 0
                if len(last_context) != 1:
                    return 0
                last_bytes = last_context[0].strip('\n')
                if int(current_bytes) == int(last_bytes):
                    return 0
                else:
                    r = int(current_bytes) - int(last_bytes)
                    return r
            else:
                return 0
        else:
            return 0
    except:
        log.error("exception when do : %s" %(traceback.format_exc()))
        return 0



def metric_init(params):
    #sample asok here
    global descriptors
    global result_asok
    osd_list = get_local_osds()
    callback_funcs = {
        0:get_oplatency_journal,
        1:get_oplatency_opw,
        2:get_oplatency_opr,
        3:get_iops,
        4:get_oplatency_apply,
        5:get_oplatency_commitcycle,
        6:get_queue_transaction,
        7:get_oplatency_subopw,
        8:get_oplatency_avgoplat,
        9:get_bytesinct,
        10:get_bytesoutct
    }
    keys = {
        0:"oplatency_journal_",
        1:"oplatency_opw_",
        2:"oplatency_opr_",
        3:"iops_",
        4:"oplatency_apply_",
        5:"oplatency_commitcycle_",
        6:"queue_transaction_",
        7:"oplatency_subopw_",
        8:"oplatency_avgoplat_",
        9:"bytesinct_",
        10:"bytesoutct_"
    }
    descripts = {
        #describe the details for each [key, value]
        0:"oplatency_journal_osdX: Shows the time spent on filejournal submit_entry to queue_completions_thru(completion) ",
        1:"oplatency_opw_osdX: Shows the time spent on receive op write(include subop ) message to op write(include subop write) commit ",
        2:"oplatency_opr_osdX: Shows the time spent on receive op read message to op read complete",
        3:"iops_osdX: Shows the num of op(include subop,or) per sample interval",
        4:"oplatency_apply_osdX: Shows the time spent on recevice op write(include subop) messaget to filestore finish op(include op)",
        5:"oplatency_commitcycle_osdX: Shows the time spent on FileStore really start sync_entry to  sync_entry commit",
        6:"queue_transaction_osdX: Shows the time spent on wating on FileStoe op_queue_reserve_throttle",
        7:"oplatency_subopw_osdX: Shows the time spent on subop message to subop write commit ",
        8:"oplatency_avgoplat_osdX: Shows the time spent on op write(include subop,op read ) message to op write(include subop write) commit or read complete ",
        9:"bytesinct_osdX: Shows bytes of osd write per sample interval",
        10:"bytesoutct: Shows bytes of osd read per sample interval"
    }
    value_types = {
        0:'float',
        1:'float',
        2:'float',
        3:'uint',
        4:'float',
        5:'float',
        6:'float',
        7:'float',
        8:'float',
        9:'uint',
        10:'uint'
    }
    formats = {
        0:'%f',
        1:'%f',
        2:'%f',
        3:'%u',
        4:'%f',
        5:'%f',
        6:'%f',
        7:'%f',
        8:'%f',
        9:'%u',
        10:'%u'
    }
    if osd_list is None:
        #print log here, level: WARN
        return []
    for id in osd_list:
        for i in xrange(0,11):
            d = {
                'name': keys[i]+NAME_PREFIX + id,
                'call_back': callback_funcs[i],
                'time_max': 90,
                'value_type': value_types[i],
                'units': 'C',
                'slope': 'both',
                'format': formats[i],
                'description': descripts[i],
                'groups': 'OSD'
            }
            descriptors.append(d)
    return descriptors
def metric_cleanup():
    pass


#This code is for debugging and unit testing
if __name__ == '__main__':
    metric_init({})

    for d in descriptors:
        v = d['call_back'](d['name'])
        print ('value for %s is '+d['format']) % (d['name'], v)
        
