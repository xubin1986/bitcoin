#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb,re,sys,time,copy,json
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
    
def isbitpriced(bit):
    #判断虚拟币是否可以换算为人民币价格
    if bit in pricekey or [ i for i in datapair[bit].keys() if i in cnyqc ]:
        return True
    else:
        return False
        
def getprofit(origprice,count,endbit):
    if endbit not in pricekey:
        try:
            transbit = [ i for i in datapair[endbit].keys() if i in pricekey ][0]
        except:
            return [endbit,origprice,0,0]
        #因为是评估价格，所以此处不计算手续费
        count = round(count*datapair[endbit][transbit][0],float)
        endbit = transbit
    routeprice = round(count*dataprice[endbit],float)
    profit = round((routeprice-origprice)/origprice*100,2)
    return [endbit,origprice,routeprice,profit]

def getorigprice(bit):
    if bit not in pricekey:
        try:
            transbit = [ i for i in datapair[bit].keys() if i in pricekey ][0]
            return round(datapair[bit][transbit][0]*dataprice[transbit],float)
        except:
            return 0
    else:
        return dataprice[bit]
        
        
def genWay(bc): 
    #import pdb;pdb.set_trace()
    way = []
    route = []
    origprice = getorigprice(bc)
    if origprice == 0:
        return 1
    for qc in datapair[bc].keys():
        profitlist = getprofit(origprice,datapair[bc][qc][0],datapair[bc][qc][-1][1])
        if profitlist[3] > minprofit:
            way.append(datapair[bc][qc]+profitlist)
        for qc1 in datapair[qc].keys():
            tmp1 = copy.deepcopy(datapair[bc][qc])
            if datapair[qc][qc1][2] in tmp1:
                continue
            tmp1.append(datapair[qc][qc1][2])
            tmp1[0] = countPrice(tmp1,qc,qc1)
            profitlist = getprofit(origprice,tmp1[0],tmp1[-1][1])
            if profitlist[3] > minprofit:
                way.append(tmp1+profitlist)
            elif profitlist[3] <= 0:
                continue
            for qc2 in datapair[qc1].keys():
                tmp2 = copy.deepcopy(tmp1)
                if datapair[qc1][qc2][2] in tmp2:
                    continue
                tmp2.append(datapair[qc1][qc2][2])
                tmp2[0] = countPrice(tmp2,qc1,qc2)
                profitlist = getprofit(origprice,tmp2[0],tmp2[-1][1])
                if profitlist[3] > minprofit:
                    way.append(tmp2+profitlist)
                elif profitlist[3] <= 0:
                    continue
                for qc3 in datapair[qc2].keys():
                    tmp3 = copy.deepcopy(tmp2)
                    if datapair[qc2][qc3][2] in tmp3:
                        continue
                    tmp3.append(datapair[qc2][qc3][2])
                    tmp3[0] = countPrice(tmp3,qc2,qc3)
                    profitlist = getprofit(origprice,tmp3[0],tmp3[-1][1])
                    if profitlist[3] > minprofit:
                        way.append(tmp3+profitlist)
                    
                    
                    
                    
                    
                    
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
                                    
    #return sorted(way,key=lambda tmp: tmp[-1],reverse=True)
    
    values = ''
    createtime = int(time.time())
    for i in way[:11]:
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
    basecurrency = datapair.keys()
    #import pdb;pdb.set_trace()
    # for i in genWay('OMG'):
        # print i
    # genWay('OMG')
    # sys.exit(1)
    for bc in basecurrency[:4]:
        genWay(bc)
        # p = Process(target=genWay,args=(bc,))
        # p.start()
    print 'Finish'
if __name__ == '__main__':
    basecount = 1
    float = 16
    minprofit = 5
    jump = 4
    pmax= 40
    rate = {"bitfinex":0.02,"hitbtc":0.02,"bittrex":0.01,"okex":0.02,"gateio":0.02,"binance":0.02,"poloniex":0.02,"ethfinex":0.02}
    main()
    print 'Finish2'