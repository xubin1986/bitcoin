#!/usr/bin/env python
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

def getData():
    sql = 'select distinct exchange from test;,,,select distinct baseCurrency from test;,,,select * from test;'
    ret = mysql(sql)
    basecurrency = [i[0] for i in ret[1]]
    exchange = [i[0] for i in ret[0]]
    data = {}
    for bc in basecurrency:
        data[bc] = {}
        for ex in exchange:
            data[bc][ex] = {}
    for row in ret[2]:
        if data[row[12]][row[11]]:
            j = 0
            for key in data[row[12]][row[11]].keys():
                if data[row[12]][row[11]][key][13] == row[13]:
                    if data[row[12]][row[11]][key][14] < row[14]:
                        data[row[12]][row[11]][key] = row
                    j=1
                    break
            if j == 0:
                data[row[12]][row[11]][row[13]] = row
        else:
            data[row[12]][row[11]][row[13]] = row
    for bc in basecurrency:
        for ex in exchange:
            if not data[bc][ex]:
                data[bc].pop(ex)
    #print data['omg']['bitfinex'].keys()
    #sys.exit(1)
    return data,basecurrency  
    
def getCoin(bit):
    if isinstance(bit,list):
        return bit[2].split('.')[1]
    else:
        return bit.split('.')[1]

    
def genWay(bc):
    #import pdb;pdb.set_trace()
    way = []
    for qc in datapair[bc].keys():
        if getCoin(datapair[bc][qc]) == 'usd' or getCoin(datapair[bc][qc]) not in basecurrency:
            way.append(datapair[bc][qc])
        else:
            if getCoin(datapair[bc][qc]) in pricekey:
                way.append(datapair[bc][qc])
            for qc1 in datapair[qc].keys():
                tmp1 = copy.deepcopy(datapair[bc][qc])
                tmp1.append(datapair[qc][qc1][2])
                tmp1[0] = round(tmp1[0] * datapair[qc][qc1][0],float)
                if getCoin(datapair[qc][qc1]) == 'usd' or getCoin(datapair[qc][qc1]) not in basecurrency:
                    way.append(tmp1)
                else:
                    if getCoin(datapair[qc][qc1]) in pricekey:
                        way.append(tmp1)
                    for qc2 in datapair[qc1].keys():
                        tmp2 = copy.deepcopy(tmp1)
                        tmp2.append(datapair[qc1][qc2][2])
                        tmp2[0] = round(tmp2[0] * datapair[qc1][qc2][0],float)
                        if getCoin(datapair[qc1][qc2]) == 'usd' or getCoin(datapair[qc1][qc2]) not in basecurrency:
                            way.append(tmp2)
                        else:
                            if getCoin(datapair[qc1][qc2]) in pricekey:
                                way.append(tmp2)
                            for qc3 in datapair[qc2].keys():
                                tmp3 = copy.deepcopy(tmp2)
                                tmp3.append(datapair[qc2][qc3][2])
                                tmp3[0] = round(tmp3[0] * datapair[qc2][qc3][0],float)
                                if getCoin(datapair[qc2][qc3]) == 'usd' or getCoin(datapair[qc2][qc3]) not in basecurrency:
                                    way.append(tmp3)
                                else:
                                    if getCoin(datapair[qc2][qc3]) in pricekey:
                                        way.append(tmp3)
                                    for qc4 in datapair[qc3].keys():
                                        tmp4 = copy.deepcopy(tmp3)
                                        tmp4.append(datapair[qc3][qc4][2])
                                        tmp4[0] = round(tmp4[0] * datapair[qc3][qc4][0],float)
                                        if getCoin(datapair[qc3][qc4]) == 'usd' or getCoin(datapair[qc3][qc4]) not in basecurrency:
                                            way.append(tmp4)
    for i in range(len(way)):
        firstbit = getCoin(way[i][1])
        lastbit = getCoin(way[i][-1]) 
        if firstbit in pricekey and lastbit in pricekey:
            way[i][0] = round(way[i][0]*dataprice[lastbit],float)
            way[i].append(dataprice[firstbit])
            profit = round((way[i][0]-way[i][-1])/way[i][-1],float)
            way[i].append(profit)
    return way
    values = ''
    for i in way:    
        values += ",('%s','%s','%s')" % (bc,i[0],str(i))
    values = re.sub(r'^,','',values)
    sql = "insert into result values %s;" % values
    if bc == '2GIVE':
        print sql
    mysql(sql)
    
    #dataway[bc] = way
    #print dataway.keys()
    

def getDataPrice():
    price = {}
    sql = "select * from coindata_price where createtime=(select max(createtime) from coindata_price);"
    ret = mysql(sql)
    pricekey = [i[1] for i in ret]
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

def getHighPair():
    #import pdb;pdb.set_trace()
    datapair = {}
    for bc in basecurrency:
        datapair[bc] = {}
        for ex in data[bc].keys():
            for qc in data[bc][ex].keys():
                try:
                    type(datapair[bc][qc])
                except:
                    datapair[bc][qc] = []    
                datapair[bc][qc].append([round(data[bc][ex][qc][6]*(1-rate[ex]),float),'%s.%s' % (ex,bc),'%s.%s' % (ex,qc)])
    #print datapair['OMG']['USDT'];sys.exit(1)
        for qc in datapair[bc].keys():
            tmp = [i[0] for i in datapair[bc][qc]]
            datapair[bc][qc] = datapair[bc][qc][tmp.index(max(tmp))]
    return datapair
def main():
    global data,basecurrency,datapair,dataprice,pricekey
    pricekey,dataprice = getDataPrice()
    data,basecurrency = getData()
    #print basecurrency;sys.exit(1)
    datapair = getHighPair()
    for i in genWay('OMG'):
        #if getCoin(i[-1]) in ['USD','USDT']:
        print i
    sys.exit(1)
    
    for bc in ['2GIVE', 'ABY', 'ADA']:
        p = Process(target=genWay,args=(bc,))
        p.start()
        #dataway[bc] = genWay(bc)
        #genWay(bc)
    print dataway.keys()
    for i in dataway['2GIVE']:
        print i
    #print datapair['eth']
            
if __name__ == '__main__':
    basecount = 1
    float = 13
    jump = 4
    pmax=10
    rate = {"bitfinex":0.02,"hitbtc":0.02,"bittrex":0.01,"okex":0.02,"gateio":0.02,"cryptopia":0.02,"poloniex":0.02}
    main()