#!/usr/bin/env python3

import json
import random
import boto3
import uuid
import math
from statistics import mean, stdev
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import cgi
import cgitb
import requests
import time

cgitb.enable()

# Initialize S3 client
s3_client = boto3.client('s3')

def process_data(h, d, t, p):
    data = s3_client.get_object(Bucket='ccharsh', Key="PAYLOAD")
    data = json.loads(data['Body'].read())
    close = data['Close']
    buy = data['Buy']
    sell = data['Sell']

    minhistory = int(h)
    shots = int(d)
    var95_list = []
    var99_list = []

    for i in range(minhistory, len(close)):
        if t == "buy" and buy[i] == 1:  # if we’re interested in Buy signals
            close_data = close[i - minhistory:i]
        elif t == "sell" and sell[i] == 1:  # if we’re interested in Sell signals
            close_data = close[i - minhistory:i]
        else:
            continue

        pct_change = [(close_data[i] - close_data[i - 1]) / close_data[i - 1] for i in range(1, len(close_data))]
        mn = mean(pct_change)
        std = stdev(pct_change)
        # generate much larger random number series with same broad characteristics
        simulated = [Decimal(str(random.gauss(mn, std))) for x in range(shots)]
        # sort and pick 95% and 99%  - not distinguishing long/short risks here
        simulated.sort(reverse=True)
        var95 = simulated[int(len(simulated) * 0.95)]
        var99 = simulated[int(len(simulated) * 0.99)]
        var95_list.append(var95)
        var99_list.append(var99)

    key = str(uuid.uuid4())
    result_data = {
        'var95': var95_list,
        'var99': var99_list,
        'dates': data['dates'][minhistory:]
    }
    s3_client.put_object(Body=json.dumps(result_data).encode('utf-8'), Bucket="ccharsh", Key=key)
    
    return key

def calculate_averages(keys):
    avg_var95 = []
    avg_var99 = []
    for key in keys:
        resp = s3_client.get_object(Bucket='ccharsh', Key=key)
        item = json.loads(resp['Body'].read())
        avg_var95.append(item['var95'])
        avg_var99.append(item['var99'])

    avg_var95 = [mean(g) for g in zip(*avg_var95)]
    avg_var99 = [mean(g) for g in zip(*avg_var99)]
    sum_var95 = mean(avg_var95)
    sum_var99 = mean(avg_var99)

    resp = s3_client.get_object(Key="HISTORY", Bucket='ccharsh')
    item = json.loads(resp['Body'].read())
    last = item.pop()
    last['var95'] = sum_var95
    last['var99'] = sum_var99
    item.append(last)
    
    pre = json.dumps(item)
    s3_client.put_object(Body=pre.encode('utf-8'), Bucket='ccharsh', Key='HISTORY')
    s3_client.put_object(Body=json.dumps(avg_var95).encode('utf-8'), Bucket='ccharsh', Key='LST95')
    s3_client.put_object(Body=json.dumps(avg_var99).encode('utf-8'), Bucket='ccharsh', Key='LST99')
    
    return {'var95': sum_var95, 'var99': sum_var99}

def generate_chart():
    hist = use_s3('g', "HISTORY")
    dates = use_s3('g', "DATES")
    lst95 = use_s3('g', "LST95")
    lst99 = use_s3('g', "LST99")
    avg95 = hist[-1]['var95']
    avg99 = hist[-1]['var99']
    
    str_d = '|'.join(dates)
    str_95 = ','.join([str(i) for i in lst95])
    str_avg95 = ','.join([str(avg95) for i in range(len(dates))])
    str_99 = ','.join([str(i) for i in lst99])
    str_avg99 = ','.join([str(avg99) for i in range(len(dates))])

    labels = "95%RiskValue|99%RiskValue|Average95%|Average99%"

    chart = f"https://image-charts.com/chart?cht=lc&chs=999x499&chd=a:{str_95}|{str_99}|{str_avg95}|{str_avg99}&chxt=x,y&chdl={labels}&chxl=0:|{str_d}&chxs=0,min90&chco=1984C5,C23728,A7D5ED,E1A692&chls=3|3|3,5,3|3,5,3"
    
    use_s3('p', key="CHART", data=chart)
    
    return 'Ok'

def use_s3(opp, key, data=None, bucket='ccharsh'):
    if opp[0] == 'p':
        jsn = json.dumps(data)
        s3_client.put_object(Body=jsn.encode('utf-8'), Bucket=bucket, Key=key)
    elif opp[0] == 'g':
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(resp['Body'].read())

# Main execution
if __name__ == "__main__":
    form = cgi.FieldStorage()
    h = form.getvalue('h')
    d = form.getvalue('d')
    t = form.getvalue('t')
    p = form.getvalue('p')
    r = int(form.getvalue('r'))
    dnss = form.getvalue('dnss').split(',')

    st = time.time()
    results = []

    def getpage(url):
        ec2_url = "http://" + url + "/aws_ec2.py"
        json_inputs = json.dumps({'h': h, 'd': d, 't': t, 'p': p})
        json_output = requests.post(ec2_url, headers={"Content-Type": "application/json"}, data=json_inputs)
        output = json.loads(json_output.text)
        result = []
        result.append(output['dates'])
        result.append(output['var95'])
        result.append(output['var99'])
        return result

    def getpages():
        with ThreadPoolExecutor() as executor:
            results = executor.map(getpage, dnss)
        return list(results)

    results = getpages()
    time_taken = time.time() - st
    cost = "$" + str(float(r) * 0.0134 * time_taken / 60)

    # Store results in S3
    result_data = {
        "results": results,
        "time_taken": time_taken,
        "cost": cost
    }
    s3_client.put_object(Body=json.dumps(result_data).encode('utf-8'), Bucket="ccharsh", Key="results.json")

    print("Content-Type: application/json")
    print()
    print(json.dumps({"status": "ok"}))
