#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""load_HMD_NonSZXA.py"""
######################################################################
## Filename:      load_HMD_NonSZXA.py
##
## Version:       2.1
## Author:        wangxintian <wangxt5@asiainfo.com>
## Created at:
##
## Description:
## 备注:
##
######################################################################

import sys
import os
import string
import copy
import time
import datetime
import getopt
import re
import signal
import logging
from socket import *
import cx_Oracle as orcl
import configparser
import shutil

import oradb



class QSub(object):
    def __init__(self, tcpClt, outPa=None,
                 reqTpl="TRADE_ID=11111111;ACTION_ID=1;DISP_SUB=4;PS_SERVICE_TYPE=HLR;MSISDN=%s;IMSI=%s;"):
        self.tcpClt = tcpClt
        self.reqTpl = reqTpl
        self.outPa = outPa

    def makeReqMsg(self, msisdn, imsi):
        self.msisdn = msisdn
        self.imsi = imsi
        reqMsg = self.reqTpl % (msisdn, imsi)
        return reqMsg

    def sendReq(self, msg):
        self.tcpClt.send(msg)

    def recvRsp(self):
        rspMsg = self.tcpClt.recv()
        aRspInfo = rspMsg.split(';')

        subInfo = ';'.join(aRspInfo[1:3])
        if self.outPa:
            outPara = ['MSISDN1', 'IMSI1'] + self.outPa
            for para in outPara:
                key = '%s%s' % (para, '=')
                for val in aRspInfo:
                    if key in val:
                        subInfo = '%s;%s' % (subInfo, val)
                        break
        else:
            subInfo = ';'.join(aRspInfo[1:])
        return subInfo

    def qrySub(self, msisdn, imsi):
        qryMsg = self.makeReqMsg(msisdn, imsi)
        self.sendReq(qryMsg)
        self.recvRsp()


class ReqOrder(object):
    def __init__(self):
        self.no = None
        self.aParamName = []
        self.dParam = {}
        self.net = None
        self.aReqMsg = []
        self.aResp = []

    def setParaName(self, aParaNames):
        self.aParamName = aParaNames

    def setPara(self, paras):
        for i, pa in enumerate(paras):
            key = self.aParamName[i]
            self.dParam[key] = pa

    def getStatus(self):
        status = ''
        for resp in self.aResp:
            status = '%s[%s:%s]' % (status, resp['status'], resp['response'])
        return status


class CmdTemplate(object):
    def __init__(self, cmdTmpl):
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'\^<(.+?)\^>'
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)

    def setMsg(self, cmdMsg):
        self.cmdTmpl = cmdMsg
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)


class KtPsTmpl(CmdTemplate):
    def __init__(self, cmdTmpl):
        # super(self.__class__, self).__init__(cmdTmpl)
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'@(.+?)@'

    def setMsg(self, tmpl):
        pass
        # self.cmdTmpl = tmpl
        # for field in tmpl:
        #     self.aVariables = re.findall(self.varExpt, self.cmdTmpl)


class KtPsOrder(ReqOrder):
    def __init__(self):
        super(self.__class__, self).__init__()
        self.aWaitPs = []
        self.dWaitNo = {}

    def getStatus(self):
        return self.aResp


class DbConn(object):
    def __init__(self, dbInfo):
        self.dbInfo = dbInfo
        self.conn = None
        # self.connectServer()

    def connectServer(self):
        if self.conn: return self.conn
        # if self.remoteServer: return self.remoteServer
        connstr = '%s/%s@%s/%s' % (self.dbInfo['dbusr'], self.dbInfo['dbpwd'], self.dbInfo['dbhost'], self.dbInfo['dbsid'])
        # print("connstr: %s" % connstr)
        try:
            self.conn = orcl.Connection(connstr)
            # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
        except Exception as e:
            logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.cfg.dbinfo['dbhost'], self.cfg.dbinfo['dbusr'], self.cfg.dbinfo['dbsid'], e)
            exit()
        return self.conn

    def prepareSql(self, sql):
        logging.info('prepare sql: %s', sql)
        cur = self.conn.cursor()
        # cur.prepare(sql)
        try:
            cur.prepare(sql)
        except orcl.DatabaseError as e:
            logging.error('prepare sql err: %s', sql)
            logging.error(e)
            return None
        # finally:
        #     pass
        return cur

    def executemanyCur(self, cur, params):
        logging.info('execute cur %s : %s', cur.statement, params)
        try:
            cur.executemany(None, params)
        except orcl.DatabaseError as e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return cur

    def fetchmany(self, cur):
        logging.debug('fetch %d rows from %s', cur.arraysize, cur.statement)
        try:
            rows = cur.fetchmany()
        except orcl.DatabaseError as e:
            logging.error('fetch sql err %s:%s ', e, cur.statement)
            return None
        return rows

    def fetchone(self, cur):
        logging.debug('fethone from %s', cur.statement)
        try:
            row = cur.fetchone()
        except orcl.DatabaseError as e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return row

    def fetchall(self, cur):
        logging.debug('fethone from %s', cur.statement)
        try:
            rows = cur.fetchall()
        except orcl.DatabaseError as e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return rows

    def executeCur(self, cur, params=None):
        logging.info('execute cur %s', cur.statement)
        logging.info(params)
        try:
            if params is None:
                cur.execute(None)
            else:
                cur.execute(None, params)
        except orcl.DatabaseError as e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return cur


class ZgClient(object):
    dSql = {}
    dSql['ProdId'] = 'select prod_id from inter.bi_imei_prod_info'
    dSql['SONBR'] = "select zg.sid.nextval from dual"
    dSql['CrmUser'] = 'select serv_id,cust_id,acct_id,zg.sid.nextval as SO_NBR,substr(serv_id,-1)+101 as servregion,substr(acct_id,-1)+101 as acctregion from zg.crm_user where phone_id=:PHONE_ID'
    dSql['UserSprom'] = "SELECT 1 FROM ZG.i_user_sprom_0%s WHERE SERV_ID=:SERV_ID AND sprom_id in(select prod_id from inter.bi_imei_prod_info) and valid_date<=to_date(to_char(add_months(trunc(sysdate),-2),'yyyymm')||'01','yyyymmdd') and expire_date>sysdate"
    dSql['InstUserParam'] = "insert into jd.i_user_param_%s(user_id,region_code,param_id,param_value,remark,valid_date,expire_date,so_nbr,oper_type,commit_date) values(:SERV_ID,:REGION_CODE,530006,'1','hmd',TO_DATE(TO_CHAR(sysdate,'yyyymm')||'01','yyyymmdd'),TO_DATE(TO_CHAR(add_months(trunc(sysdate),1),'yyyymm')||'01','yyyymmdd'),:SO_NBR,1,sysdate)"
    dSql['InstDataIndex'] = "insert into jd.i_data_index(cust_id,acct_id,user_id,up_field,region_code,county_code,commit_date,so_nbr,remark,done_code,busi_code) values(:CUST_ID,:ACCT_ID,:SERV_ID,'0000000000000000000000000000001000000000000000000000000000000000',:REGION_CODE,1000,sysdate,:SO_NBR,'hmd',:SO_NBR,0)"

    def __init__(self):
        # self.dNetInfo = netInfo
        # self.orderTablePre = 'i_provision'
        self.conn = main.conn
        self.dCur = {}

    def getCurbyName(self, curName):
        '''get cursor by name'''
        if curName in self.dCur: return self.dCur[curName]
        curPre = curName[:9]
        if (curPre != 'UserSprom') and (curPre != 'InstUserP') and (curName not in self.dSql):
            logging.error('no cursor %s', curName)
            return None
        sql = ''
        if curPre == 'UserSprom':
            namePre = curName[:9]
            regionCode = curName[9:]
            sql = self.dSql[namePre] %  regionCode
        elif curPre == 'InstUserP':
            namePre = curName[:13]
            regionCode = curName[13:]
            sql = self.dSql[namePre] %  regionCode
        else:
            sql = self.dSql[curName]
        cur = self.conn.prepareSql(sql)
        self.dCur[curName] = cur
        return cur

    def getCrmUser(self, order):
        '''get serv_id,cust_id,acct_id from crm_user by phone_id'''
        # sql = self.__class__.dSql['RegionCode']
        cur = self.getCurbyName('CrmUser')
        if 'PHONE_ID' not in order:
            logging.error('data error: no phone_id')
            return False
        dVar = {'PHONE_ID':order['PHONE_ID']}
        self.conn.executeCur(cur, dVar)
        row = self.conn.fetchone(cur)
        # order.dParam['REGION_CODE'] = '100'
        if row:
            order['SERV_ID'] = row[0]
            order['CUST_ID'] = row[1]
            order['ACCT_ID'] = row[2]
            order['SO_NBR'] = row[3]
            order['SERV_REGION'] = row[4]
            order['ACCT_REGION'] = row[5]
            return order
        else:
            return False

    def isSprom(self, order):
        ''' if or not have product in sprom '''
        curName = 'UserSprom%s' % order['SERV_REGION']
        cur = self.getCurbyName(curName)
        dVar = {'SERV_ID': order['SERV_ID']}
        self.conn.executeCur(cur, dVar)
        row = self.conn.fetchone(cur)
        return row

    def save(self, order):
        '''save hmd order to jd.i_user_param and jd.i_data_index'''
        curUserName = 'InstUserParam%s' % main.curMon
        curUserPara = self.getCurbyName(curUserName)
        curDataIndex = self.getCurbyName('InstDataIndex')
        dVarUP = {'SERV_ID': order['SERV_ID'],
                  'REGION_CODE': order['SERV_REGION'],
                  'SO_NBR': order['SO_NBR']}
        dVarDI = {'SERV_ID': order['SERV_ID'],
                  'CUST_ID': order['CUST_ID'],
                  'ACCT_ID': order['ACCT_ID'],
                  'REGION_CODE': order['ACCT_REGION'],
                  'SO_NBR': order['SO_NBR']
                  }
        self.conn.executeCur(curUserPara, dVarUP)
        self.conn.executeCur(curDataIndex, dVarDI)
        self.conn.conn.commit()


class GetRate(object):
    '''get rate from table'''
    def __init__(self, table):
        self.table = table


class RateAudit(object):
    '''audit rate from 3 tables
    audit_code:
        error_rate          税率错误
        duplicate_item      科目重复
        empty_rate          空税率
        conflict_item       科目冲突
        consistent          稽核一致

    '''
    SAVE_SQL = 'insert into zg.rate_audit(audit_code ,item_code , item_name, audit_date ) values(:audit_code ,:item_code , :ITEM_NAME, sysdate)'
    ITEMNAME_SQL ='select acct_item_type_name as item_name from base.bs_acct_item_type where acct_item_type_id=:item_code'

    def __init__(self):
        self.dRate = {}
        self.consistent = True
        self.a_duplicate_item = set()
        self.a_error_rate = set()
        self.a_empty_rate = set()
        self.a_inconsistent_item = set()

    def get_rate(self):
        for tab,db_name in main.d_table.items():
            a_rates = main.d_table_rate[tab]
            d_itemrate = {}
            for ra in a_rates:
                d_itemrate[ra] = set()
            self.dRate[tab] = d_itemrate

            db = main.dDbcn[db_name]
            sql = main.d_table_sql[tab]
            with db:
                d_rates = db.select(sql)
                logging.info('get %d rate from %s', len(d_rates), tab)
                self.make_rate_set(tab,d_rates)

    def make_rate_set(self, tab, d_rates):
        a_rate_key = list(main.d_table_rate[tab])
        logging.debug('rate key: %s', a_rate_key)
        d_itemrate = self.dRate[tab]
        d_rate_map = None
        if tab in main.d_table_rate_convert:
            d_rate_map = main.d_rate_feeid_map
        for ra in d_rates:
            # logging.debug('row:%s', ra)
            item = ra['ITEM']
            rate = d_rate_map[ra['RATE']] if d_rate_map else ra['RATE']
            if rate not in a_rate_key:
                self.consistent = False
                self.a_error_rate.add(rate)
                continue
            for r,ir in d_itemrate.items():
                if item in ir:
                    self.consistent = False
                    self.a_duplicate_item.add(item)
                    continue
            d_itemrate[rate].add(item)
        logging.info('error rate: %s', self.a_error_rate)
        logging.info('duplicate_item: %s', self.a_duplicate_item)

        # check empty rate
        for rk in a_rate_key:
            if len(d_itemrate[rk]) == 0:
                self.consistent = False
                self.a_empty_rate.add(rk)
        logging.info('empty_rate: %s', self.a_empty_rate)

    def audit(self):
        a_rates = set()
        for tab,rates in main.d_table_rate.items():
            a_rates.update(rates)

        for r in a_rates:
            a_check = []
            # for tab,rates in main.d_table_rate.items():
            #     if r in rates:
            #         a_check.append(self.dRate[tab][r])
            for tab,rate_sets in self.dRate.items():
                if r in rate_sets:
                    a_check.append(self.dRate[tab][r])

            # base_item = a_check[0]
            for i in range(0,len(a_check)):
                base_item = a_check[i]
                for j in range(i+1, len(a_check)):
                    if a_check[j] == base_item:
                        continue
                    exclusive = base_item ^ a_check[j]
                    self.consistent = False
                    self.a_inconsistent_item.update(exclusive)

    def save_set(self, code, data_set):
        logging.info('save %s data set', code)
        db = main.dDbcn['db_main']
        with db.cursor(RateAudit.SAVE_SQL) as cur:
            for a in data_set:
                d_result = {'audit_code':code, 'item_code':a, 'ITEM_NAME':''}
                if code in ['duplicate_item','conflict_item']:
                    item_name = self.get_item_name(a)
                    if item_name:
                        d_result.update(item_name)
                    # d_result['ITEM_NAME'] = self.get_item_name(a)
                cur._update(d_result)

    def get_item_name(self, item_code):
        logging.debug('query item name for %s', item_code)
        db = main.dDbcn['db_main']
        d_itemcode = {'item_code':item_code}
        item_name = ''
        item_name = db.select_one(RateAudit.ITEMNAME_SQL, d_itemcode, True)
        return item_name

    def save_result(self):
        '''
        error_rate          税率错误
        duplicate_item      科目重复
        empty_rate          空税率
        conflict_item       科目冲突
        consistent          稽核一致'''
        if self.consistent:
            logging.info('All rate is consistent in 3 tables.')
            self.save_set('consistent', {0})
        else:
            if self.a_duplicate_item:
                logging.info('duplicate_item count %d', len(self.a_duplicate_item))
                logging.info('duplicate_item：%s', self.a_duplicate_item)
                self.save_set('duplicate_item', self.a_duplicate_item)
            if self.a_error_rate:
                logging.info('error_rate count %d', len(self.a_error_rate))
                logging.info('error_rate：%s', self.a_error_rate)
                self.save_set('error_rate', self.a_error_rate)
            if self.a_empty_rate:
                logging.info('empty_rate count %d', len(self.a_empty_rate))
                logging.info('empty_rate：%s', self.a_empty_rate)
                self.save_set('empty_rate', self.a_empty_rate)
            if self.a_inconsistent_item:
                logging.info('conflict_item count %d', len(self.a_inconsistent_item))
                logging.info('conflict_item：%s', self.a_inconsistent_item)
                self.save_set('conflict_item', self.a_inconsistent_item)

    def start(self):
        self.get_rate()
        self.audit()
        self.save_result()


class Builder(object):
    '''Builder Class for check and save hmd'''
    def __init__(self, main):
        self.main = main
        self.orderDsName = main.dsIn
        self.inFileName = main.inFileName
        self.backFileName = '%s.bak' % self.inFileName
        self.errFileName = '%s.err' % self.inFileName
        self.errFile = os.path.join(self.main.dirInput, self.errFileName)
        self.fErr = None
        self.orderDs = None
        self.client = ZgClient()

    def openDs(self):
        '''open input file to process'''
        if self.orderDs: return self.orderDs
        logging.info('open ds %s', self.orderDsName)
        self.orderDs = self.main.openFile(self.orderDsName, 'r')
        if self.orderDs is None:
            logging.fatal('Can not open orderDs file %s.', self.orderDsName)
            exit(2)
        return self.orderDs

    def closeFile(self):
        if self.orderDs:
            self.orderDs.close()
            self.orderDs = None
        if self.fErr:
            self.fErr.close()
            self.fErr = None

    def openErr(self):
        '''open error file for error orderes writed into'''
        if self.fErr: return self.fErr
        logging.info('open error file: %s', self.errFile)
        self.fErr = self.main.openFile(self.errFile, 'w')
        if self.fErr is None:
            logging.fatal('Can not open error file %s.', self.errFileName)
            exit(2)
        return self.fErr

    def backFile(self):
        '''backup hmd file to back dir'''
        if os.path.exists(self.orderDsName):
            shutil.copy(self.orderDsName, self.main.dirBack)
        # os.rename(fileWkRsp, fileOutRsp)

    def saveErrOrder(self, line):
        try:
            self.fErr.write('%s%s' % (line, os.linesep))
        except IOError as e:
            logging.error('write errfile failure')

    def start(self):
        # self.backFile()
        self.openDs()
        self.openErr()
        logging.debug('load hmd %s.', self.orderDsName)
        for line in self.orderDs:
            line = line.strip()
            logging.debug(line)
            if not line:
                continue
            if line[0] == '#':
                continue
            aParams = line.split(',')

            order = {'PHONE_ID': aParams[0]}
            try:
                if not self.client.getCrmUser(order):
                    logging.error('no phone_id or no crmuser: %s ', line)
                    self.saveErrOrder(line)
                    continue
                if self.client.isSprom(order):
                    self.client.save(order)
            except Exception as e:
                logging.error('process failure: %s ', line)
                logging.error(e)
                self.saveErrOrder(line)
        self.clearAll()

    def clearFile(self):
        '''clear all files which been done'''
        logging.info('clear files')
        if os.path.exists(self.orderDsName):
            os.remove(self.orderDsName)
        if os.path.isfile(self.errFile):
            os.rename(self.errFile, os.path.join(self.main.dirBack, self.errFileName))

    def clearAll(self):
        '''clear context env for cur file connection backfile'''
        for cur in self.client.dCur:
            dbCur = self.client.dCur[cur]
            if dbCur:
                self.client.dCur[cur].close()
        self.client.dCur.clear()
        main.conn.conn.close()
        main.conn = None
        self.closeFile()
        # self.clearFile()


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None

    def start(self):
        self.factory.loadCmd()
        if not self.factory.makeNet():
            logging.error('make net error, exit.')
            return -1
        self.factory.openDs()
        self.factory.makeOrderFildName()
        self.fRsp = self.factory.openRsp()
        i = 0
        while not self.shutDown:
            logging.debug('timeer %f load order', time.time())
            order = self.factory.makeOrder()

            if order is None:
                logging.info('load all orders,')
                break
            # logging.debug(order.dParam)
            client = order.net
            i += 1
            # client.connectServer()
            logging.info('send order:')
            client.sendOrder(order)
            # client.recvResp(order)
            # client.saveResp(order)
            # client.remoteServer.close()
            self.factory.saveResp(order)
        self.factory.closeDs()
        self.factory.resp.close()
        logging.info('all order completed.')


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.dDbcn = {}
        self.conn = None
        self.d_table = {}
        self.d_table_sql = {}
        self.d_table_rate = {}
        self.d_table_rate_convert = {}
        self.d_rate_feeid_map = {}

    def checkArgv(self):
        self.dirBin = os.path.dirname(os.path.abspath(__file__))
        self.dirBase = os.path.dirname(self.dirBin)
        self.appName = os.path.basename(self.Name)
        self.appNameBody, self.appNameExt = os.path.splitext(self.appName)

        # if self.argc > 1:
        #     self.inFileName = sys.argv[1]

    def parseWorkEnv(self):
        # self.dirBin = os.path.join(self.dirBase, 'bin')
        # self.dirLog = os.path.join(self.dirBase, 'log')
        # self.dirCfg = os.path.join(self.dirBase, 'bin')
        self.dirCfg = self.dirBin
        self.dirLog = self.dirBin
        # self.dirTpl = os.path.join(self.dirBase, 'template')
        # self.dirLib = os.path.join(self.dirBase, 'lib')
        # self.dirInput = os.path.join(self.dirBase, 'input')
        # self.dirBack = os.path.join(self.dirBase, 'back')
        # self.dirOutput = os.path.join(self.dirBase, 'output')
        # self.dirWork = os.path.join(self.dirBase, 'work')

        # self.today = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.curMon = self.today[:6]
        # 上月
        today = datetime.date.today()
        last_month = today + datetime.timedelta(days=-today.day)
        # last_month.month                                     # 月份，out: 1
        self.lastMonth = datetime.datetime.strftime(last_month, "%Y%m")  # out: '201910'

        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logNamePre = '%s_%s' % (self.appNameBody, self.today)
        # outFileName = '%s_%s' % (os.path.basename(self.inFileName), self.today)
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        # self.logPre = os.path.join(self.dirLog, logNamePre)
        # self.outFile = os.path.join(self.dirOutput, outFileName)
        # self.cmdFile = os.path.join(self.dirTpl, self.cmdFileName)
        # if self.inFileName:
        #     self.dsIn = os.path.join(self.dirInput, self.inFileName)

    def readCfg(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read(self.cfgFile)
        self.dDbInfo = {}

        # read db info
        db_sections = [x for x in self.cfg.sections() if x[:3] == 'db_']
        if not db_sections:
            print('there is no db info in confige file, exit.')
            exit(-1)
        for db in db_sections:
            d_dbinfo = {}
            for item in self.cfg.items(db):
                d_dbinfo[item[0]] = item[1]
            self.dDbInfo[db] = d_dbinfo

        # read rate audit conf
        for item in self.cfg.items('table_map'):
            self.d_table[item[0]] = item[1]
        for item in self.cfg.items('table_sql'):
            self.d_table_sql[item[0]] = item[1]
        for item in self.cfg.items('table_rate'):
            # self.d_table_rate[item[0]] = map(int, item[1].split(','))
            self.d_table_rate[item[0]] = [int(x) for x in item[1].split(',')]
        for item in self.cfg.items('table_rate_convert'):
            self.d_table_rate_convert[item[0]] = item[1]
        for item in self.cfg.items('rate_feeid_map'):
            self.d_rate_feeid_map[int(item[0])] = int(item[1])

    def usage(self):
        print("Usage: %s" % self.appName)
        # print("example:   %s %s" % (self.appName,'HMD_NonSZXA_201910.dat'))
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError as e:
            logging.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    def connectServer(self):
        logging.info('make all db needed.')
        for db,info in self.dDbInfo.items():
            if db in self.dDbcn:
                continue
            logging.debug(info)
            self.dDbcn[db] = oradb.Db(info)
        return self.dDbcn

    def prepareSql(self, sql):
        logging.info('prepare sql: %s', sql)
        cur = self.conn.cursor()
        try:
            cur.prepare(sql)
        except orcl.DatabaseError as e:
            logging.error('prepare sql err: %s', sql)
            return None
        return cur

    def makeBuilder(self):
        builder = RateAudit()
        return builder

        # if self.facType == 't':
        #     return self.makeTableFactory()
        # elif self.facType == 'f':
        #     return self.makeFileFactory()

    @staticmethod
    def createInstance(module_name, class_name, *args, **kwargs):
        module_meta = __import__(module_name, globals(), locals(), [class_name])
        class_meta = getattr(module_meta, class_name)
        obj = class_meta(*args, **kwargs)
        return obj

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()
        self.readCfg()

        # self.logLevel = eval(logLevel)
        self.logLevel = eval('logging.%s' % self.cfg.get("main", "loglevel"))
        # print('loglevel: %s, logfile: %s' %(self.logLevel, self.logFile))
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...', self.appName)
        print('logfile: %s' % self.logFile)

        self.connectServer()
        with self.dDbcn['db_main']:
            builder = self.makeBuilder()
            builder.start()


def createInstance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj

# main here
if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.appName)
