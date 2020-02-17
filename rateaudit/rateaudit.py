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
        # self.fCmd = None
        # self.caseDs = None
        # self.netType = None
        # self.netCode = None
        self.conn = None
        # self.psId = None
        self.inFileName = None
        self.dsIn = None

    def checkArgv(self):
        self.dirBin = os.path.dirname(os.path.abspath(__file__))
        self.dirBase = os.path.dirname(self.dirBin)
        self.appName = os.path.basename(self.Name)
        self.appNameBody, self.appNameExt = os.path.splitext(self.appName)

        if self.argc > 1:
            self.inFileName = sys.argv[1]
        # if self.argc < 3:
        #     self.usage()
        # # self.checkopt()
        # argvs = sys.argv[1:]
        # self.facType = 'f'
        # try:
        #     opts, arvs = getopt.getopt(argvs, "t:p:")
        # except getopt.GetoptError, e:
        #     orderMode = 't'
        #     print 'get opt error:%s. %s' % (argvs,e)
        #     # self.usage()
        # for opt, arg in opts:
        #     # print 'opt: %s' % opt
        #     if opt == '-t':
        #         self.facType = 't'
        #         self.cmdFileName = arg
        #     elif opt == '-p':
        #         self.psId = arg
        # if self.facType == 'f':
        #     self.cmdFileName = arvs[0]
        #     self.inFileName = arvs[1]
        # else:
        #     self.dsIn = arvs[0]

    def parseWorkEnv(self):
        # self.dirBin = os.path.join(self.dirBase, 'bin')
        # self.dirLog = os.path.join(self.dirBase, 'log')
        # self.dirCfg = os.path.join(self.dirBase, 'bin')
        self.dirCfg = self.dirBin
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
        # self.logFile = os.path.join(self.dirLog, logName)
        # self.logPre = os.path.join(self.dirLog, logNamePre)
        # self.outFile = os.path.join(self.dirOutput, outFileName)
        # self.cmdFile = os.path.join(self.dirTpl, self.cmdFileName)
        # if self.inFileName:
        #     self.dsIn = os.path.join(self.dirInput, self.inFileName)

    def readCfg(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read(self.cfgFile)
        self.dDbInfo = {}
        self.dNetTypes = {}

        if 'db' not in self.cfg.sections():
            # logging.fatal('there is no db info in confige file')
            print('there is no db info in confige file, exit.')
            exit(-1)
        for inf in self.cfg.items('db'):
            self.dDbInfo[inf[0]] = inf[1]
        # print(self.dDbInfo)

        self.dirInput = self.cfg.get("main", "filedir")
        self.dirBack = self.cfg.get("main", "bakdir")
        self.dirLog = self.dirInput

        if not self.inFileName:
            self.inFileName = 'HMD_NonSZXA_%s.dat' % self.lastMonth
        self.dsIn = os.path.join(self.dirInput, self.inFileName)
        self.logFile = os.path.join(self.dirLog, '%s_%s.log' % (self.appNameBody, self.today))

        # # logging.info("load nettype and netinfo")
        # for sec in self.cfg.sections():
        #     # print(sec)
        #     # print(self.cfg.options(sec))
        #     if "nettype" in self.cfg.options(sec):
        #         nt = self.cfg.get(sec,"nettype")
        #         netInfo = {}
        #         for ntin in self.cfg.items(sec):
        #             netInfo[string.upper(ntin[0])] = ntin[1]
        #         if nt in self.dNetTypes:
        #             self.dNetTypes[nt].append(netInfo)
        #         else:
        #             self.dNetTypes[nt] = [netInfo]

    def usage(self):
        print("Usage: %s [HMD_NonSZXA_file]" % self.appName)
        print("example:   %s %s" % (self.appName,'HMD_NonSZXA_201910.dat'))
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError as e:
            logging.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    def connectServer(self):
        if self.conn is not None: return self.conn
        # self.dbinfo['connstr'] = '%s/%s@%s/%s' % (
        # self.dbinfo['dbusr'], self.dbinfo['dbpwd'], self.dbinfo['dbhost'], self.dbinfo['dbsid'])
        # if "db" not in self.cfg.sections():
        #     logging.error("no db configer")
        #     exit(-1)
        self.conn = DbConn(self.dDbInfo)
        self.conn.connectServer()
        return self.conn

    # def connDb(self):
    #     if self.conn: return self.conn
    #     try:
    #         connstr = self.cfg.dbinfo['connstr']
    #         self.conn = orcl.Connection(connstr)
    #         # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
    #         # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
    #         # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
    #     except Exception, e:
    #         logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.cfg.dbinfo['dbhost'], self.cfg.dbinfo['dbusr'], self.cfg.dbinfo['dbsid'], e)
    #         exit()
    #     return self.conn

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
        builder = Builder(self)
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
        print('loglevel: %s, logfile: %s' %(self.logLevel, self.logFile))
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...', self.appName)
        print('logfile: %s' % self.logFile)

        self.connectServer()
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
