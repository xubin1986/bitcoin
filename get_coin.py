#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb,re,sys,time,copy,json
import multiprocessing
from multiprocessing import Process

def mysql(sql):
    #import pdb;pdb.set_trace()
    dbhost = 'localhost'
    dbuser = 'root'
    dbpassword = '1qaz2wsx'
    db = 'coin'
    try:    
        sqlconnect = MySQLdb.connect(dbhost,dbuser,dbpassword,db)
        cursor = sqlconnect.cursor()
    except:
        print "Failed to connect database"
        sys.exit(1)
    try:
        if not re.search(',,,',sql):
            sql_lower = sql.lower()
            res = cursor.execute(sql)
            if 'insert' in sql_lower:
                res = int(sqlconnect.insert_id())                    
            elif 'select' in sql_lower or 'desc' in sql_lower:
                res = cursor.fetchall()
            sqlconnect.commit()
            return res
        else:
            output = []
            for i in sql.split(',,,'):
                sql_lower = i.lower()
                res = cursor.execute(i)
                if 'insert' in sql_lower:
                    res = int(sqlconnect.insert_id())
                elif 'select' in sql_lower:
                    res = cursor.fetchall()
                output.append(res)
            sqlconnect.commit()
            return output
    except Exception as e:
        print str(e)
        sqlconnect.close()
        sys.exit(1)
    finally:
        sqlconnect.close()


def getHighPair():
    #格式化所有交易对为[[price,source,target]]
    #import pdb;pdb.set_trace()
    sql = 'select * from coindata_tickers where codeid=1;,,,select distinct quotcurrency from coindata_tickers where codeid=1;'
    ret = mysql(sql)
    datapair = {}
    cnyqc = [i[0] for i in ret[1]]
    for row in ret[0]:
        #记录正方向的价格
        bc = row[9]
        ex = row[11]
        qc = row[10]
        last = row[3]
        if not last:
            continue
        try:
            type(datapair[bc])
        except:
            datapair[bc] = {}
        try:
            type(datapair[bc][qc])
        except:
            datapair[bc][qc] = []
        price = round(last*(1-rate[ex]),float)
        source = [ex,bc]
        target = [ex,qc]
        datapair[bc][qc].append([price,source,target])
        #记录反方向的价格
        if row[6] != 0:
            bc = row[10]
            ex = row[11]
            qc = row[9]
            last = round(1/last,float)
            try:
                type(datapair[bc])
            except:
                datapair[bc] = {}
            try:
                type(datapair[bc][qc])
            except:
                datapair[bc][qc] = []
            price = round(last*(1-rate[ex]),float)
            source = [ex,bc]
            target = [ex,qc]
            datapair[bc][qc].append([price,source,target])
    #获取所有交易对中最高价格的交易对，并保留
    for bc in datapair.keys():
        for qc in datapair[bc].keys():
            tmp = [i[0] for i in datapair[bc][qc]]
            datapair[bc][qc] = datapair[bc][qc][tmp.index(max(tmp))]
    return datapair,cnyqc

def countPrice(tmpway,bc,qc):
    if tmpway[-2][0] == tmpway[-1][0]:
        #交易所相同的时候计算换币的手续费
        price = round(tmpway[0] * datapair[bc][qc][0],float)
    else:
        #两跳之间交易所不同的时候，需要计算换交易所的手续费
        price = round(tmpway[0] * datapair[bc][qc][0] * (1-rate[tmpway[-2][0]]),float)
    return price    
        
def getstepprice(tmp,qc):
    bc = tmp[-1][1]
    try:
        price = datapair[bc][qc][0]
        return round(tmp[0] * price,float)
    except:
        return 0
        
def genWay(bc): 
    #import pdb;pdb.set_trace()
    if bc not in datapair.keys():
        print 'can not statistic for %s' % bc
        return 1
    way = []
    for qc in datapair[bc].keys():
        tmp0 = copy.deepcopy(datapair[bc][qc])
        for qc1 in datapair[qc].keys():
            tmp1 = copy.deepcopy(tmp0)
            tmp1.append(datapair[qc][qc1][2])
            tmp1[0] = countPrice(tmp1,qc,qc1)
            if tmp1[-1][1] == bc:
                if tmp1[0] > 1:
                    way.append(tmp1)
                else:
                    continue
            else:
                price = getstepprice(tmp1,bc)
                if price <= 1:
                    continue
            for qc2 in datapair[qc1].keys():
                tmp2 = copy.deepcopy(tmp1)
                tmp2.append(datapair[qc1][qc2][2])
                tmp2[0] = countPrice(tmp2,qc1,qc2)
                if tmp2[-1][1] == bc:
                    if tmp2[0] > 1:
                        way.append(tmp2)
                    else:
                        continue
                else:
                    price = getstepprice(tmp2,bc)
                    if price <= 1:
                        continue
                for qc3 in datapair[qc2].keys():
                    tmp3 = copy.deepcopy(tmp2)
                    tmp3.append(datapair[qc2][qc3][2])
                    tmp3[0] = countPrice(tmp3,qc2,qc3)
                    if tmp3[-1][1] == bc:
                        if tmp3[0] > 1:
                            way.append(tmp3)
                    else:
                        continue
                    
                    
                    
                    
                    
                    # if qc3 in basecurrency:
                        # for qc4 in datapair[qc3].keys():
                            # tmp4 = copy.deepcopy(tmp3)
                            # tmp4.append(datapair[qc3][qc4][2])
                            # tmp4[0] = countPrice(tmp4,qc3,qc4)
                            #if isbitpriced(tmp4[-1][1]):
                            # way.append(tmp4)
                            # if qc4 in basecurrency:
                                # for qc5 in datapair[qc4].keys():
                                    # tmp5 = copy.deepcopy(tmp4)
                                    # tmp5.append(datapair[qc4][qc5][2])
                                    # tmp5[0] = countPrice(tmp5,qc4,qc5)
                                    # way.append(tmp5)
                                    
    return sorted(way,key=lambda tmp: tmp[0],reverse=True)
    
    values = ''
    createtime = int(time.time())
    for i in sorted(way,key=lambda tmp: tmp[-1],reverse=True)[:100]:
        startexchage = i[1][0]
        startcoin = i[1][1]
        endexchange = i[-5][0]
        endcoin = i[-5][1]
        startprice = i[-3]
        endcount = i[0]
        endprice = i[-2]
        profit = i[-1]
        routes = json.dumps({"route":i[1:-4]})
        values += ",(NULL,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (startexchage,startcoin,endexchange,endcoin,startprice,endcount,endprice,profit,routes,'2',createtime)
    values = re.sub(r'^,','',values)
    sql = "insert into coindata_routes values %s;" % values
    mysql(sql)
    print 'finish statistic for %s' % bc
    
    #dataway[bc] = way
    #print dataway.keys()
    

def getDataPrice():
    price = {}
    sql = "select * from coindata_price where createtime=(select max(createtime) from coindata_price);"
    ret = mysql(sql)
    pricekey = [i[0] for i in ret]
    pricevalue = [i[2] for i in ret]
    for i in range(len(pricekey)):
        price[pricekey[i]] = pricevalue[i]
    price['USD'] = 6.3492
    price['EUR'] = 7.57119
    price['JPY'] = 0.057744
    price['GBP'] = 8.5207
    pricekey.append('USD')
    pricekey.append('EUR')
    pricekey.append('JPY')
    pricekey.append('GBP')
    return pricekey,price
    
def main():
    global basecurrency,pricekey,dataprice,datapair,cnyqc
    pricekey,dataprice = getDataPrice()
    datapair,cnyqc = getHighPair()
    #basecurrency = datapair.keys()
    #import pdb;pdb.set_trace()
    for i in genWay('BTC'):
        print i
    sys.exit(1)
    
    # for bc in runbc[:4]:
        #genWay(bc)
        # p = Process(target=genWay,args=(bc,))
        # p.start()
        # p.join()
    pool = multiprocessing.Pool(processes=8)
    for bc in runbc:
        pool.apply_async(genWay, (bc, ))
    pool.close()
    pool.join()
    print 'Finish'
    sql = "update coindata_routes set codeid=0 where codeid=1;,,,update coindata_routes set codeid=1 where codeid=2;"
    mysql(sql)
if __name__ == '__main__':
    float = 16
    runbc = ['BTC','ETH','XRP','BCH','EOS','LTC','XLM','ADA','TRX','MIOTA','USDT','NEO','DASH','BNB','VEN','ETC','XEM','OKB','HT','OMG','QTUM','ONT','ZEC','ICX','LSK','DCR','BCN','ZIL','BTG','XET','BTM','SC','ZRX','XVG']
    rate = {"bitfinex":0.02,"hitbtc":0.02,"bittrex":0.01,"okex":0.02,"gateio":0.02,"binance":0.02,"poloniex":0.02,"ethfinex":0.02}
    main()