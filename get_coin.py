#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb,re,xlwt,sys,time,copy
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
    #import pdb;pdb.set_trace()
    sql = 'select * from test;'
    ret = mysql(sql)
    datapair = {}
    for row in ret:
        bc = row[12]
        ex = row[11]
        qc = row[13]
        last = row[6]
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
    for bc in datapair.keys():
        for qc in datapair[bc].keys():
            tmp = [i[0] for i in datapair[bc][qc]]
            datapair[bc][qc] = datapair[bc][qc][tmp.index(max(tmp))]
    return datapair
    
        
def countPrice(tmpway,bc,qc):
    if tmpway[-2][0] == tmpway[-1][0]:
        price = round(tmpway[0] * datapair[bc][qc][0],float)
    else:
        price = round(tmpway[0] * datapair[bc][qc][0] * (1-rate[tmpway[-2][0]]),float)
    return price
    
def isbitpriced(bit):
    if bit in pricekey or (bit in basecurrency and 'USDT' in datapair[bit].keys()):
        return True
    else:
        return False
        
        
def genWay(bc): 
    #import pdb;pdb.set_trace()
    way = []
    for qc in datapair[bc].keys():
        #if isbitpriced(datapair[bc][qc][-1][1]):
        way.append(datapair[bc][qc])
        if qc in basecurrency:
            for qc1 in datapair[qc].keys():
                tmp1 = copy.deepcopy(datapair[bc][qc])
                tmp1.append(datapair[qc][qc1][2])
                tmp1[0] = countPrice(tmp1,qc,qc1)
                #if isbitpriced(tmp1[-1][1]):
                way.append(tmp1)
                if qc1 in basecurrency:
                    for qc2 in datapair[qc1].keys():
                        tmp2 = copy.deepcopy(tmp1)
                        tmp2.append(datapair[qc1][qc2][2])
                        tmp2[0] = countPrice(tmp2,qc1,qc2)
                        #if isbitpriced(tmp2[-1][1]):
                        way.append(tmp2)
                        if qc2 in basecurrency:
                            for qc3 in datapair[qc2].keys():
                                tmp3 = copy.deepcopy(tmp2)
                                tmp3.append(datapair[qc2][qc3][2])
                                tmp3[0] = countPrice(tmp3,qc2,qc3)
                                #if isbitpriced(tmp3[-1][1]):
                                way.append(tmp3)
                                if qc3 in basecurrency:
                                    for qc4 in datapair[qc3].keys():
                                        tmp4 = copy.deepcopy(tmp3)
                                        tmp4.append(datapair[qc3][qc4][2])
                                        tmp4[0] = countPrice(tmp4,qc3,qc4)
                                        #if isbitpriced(tmp4[-1][1]):
                                        way.append(tmp4)
    #return way
    #import pdb;pdb.set_trace()
    route = []
    if not isbitpriced(bc):
        for i in way:
            route.append(i+[0,0])
        return route
    for i in range(len(way)):
        endbit = way[i][-1][1]
        if not isbitpriced(endbit):
            route.append(way[i]+[0,0])
            continue
        if bc in pricekey:
            origprice = dataprice[bc]
        else:
            origprice = round(datapair[bc]['USDT'][0] * dataprice['USDT'],float)

        if endbit not in pricekey:
            way.append(datapair[endbit]['USDT'][2])
            #因为是评估价格，所以此处不计算手续费
            way[i][0] = round(way[i][0]*datapair[endbit]['USDT'][0],float)
            endbit = 'USDT'
        routeprice = round(way[i][0]*dataprice[endbit],float)
        profit = round((routeprice-origprice)/origprice*100,2)
        way[i][0] = routeprice
        if profit > 0:
            route.append(way[i]+[origprice,profit])
    #return sorted(way,key=lambda tmp: tmp[0],reverse=True)[0:19]
    return sorted(route,key=lambda tmp: tmp[-1],reverse=True)
    
    values = ''
    for i in sorted(way,key=lambda tmp: tmp[0],reverse=True)[0:19]:
        values += ",NULL,('%s','%s','%s')" % (bc,i[0],','.join(i[1:-2]))
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
    pricekey.append('USD')
    pricekey.append('EUR')
    pricekey.append('JPY')
    return pricekey,price
    
def main():
    global basecurrency,pricekey,dataprice,datapair
    pricekey,dataprice = getDataPrice()
    datapair = getHighPair()
    basecurrency = datapair.keys()
    for i in genWay('OMG'):
        print i
    sys.exit(1)
    
    for bc in basecurrency:
        p = Process(target=genWay,args=(bc,))
        p.start()

if __name__ == '__main__':
    basecount = 1
    float = 13
    jump = 4
    pmax= 40
    rate = {"bitfinex":0.02,"hitbtc":0.02,"bittrex":0.01,"okex":0.02,"gateio":0.02,"cryptopia":0.02,"poloniex":0.02}
    main()