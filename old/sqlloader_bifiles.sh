#!/bin/sh
#. /boss4/pkghdjk2/intf2/conf/env.cfg
. /app/conf/env.cfg

FILEDIR=/data01/in/bi_imei/ 
DBNAME=srv_zw2
USERNAME=inter
#DBPASSWORD=`/home/mmadm/bin/getpw.sh db inter`
DBPASSWORD='Rn4GmQ#p!Z'
CTLDIR=/app/shell/crontab/ctl 
BAKDIR=/data01/in/bi_imei/bak

curdate=`date +%Y%m`
beforeMon=`date +%Y%m|awk '{s=$1-1; if (substr($1,5,2)=="01") s=s-88; print s}'`


#神A白,神A黑,非神A黑文件名变量
sabfname=BMD_SZXA_$beforeMon.dat
sahfname=HMD_SZXA_$beforeMon.dat
fsafname=HMD_NonSZXA_$beforeMon.dat

cd $FILEDIR

if [ -f $sabfname ];then
        sqlldr userid=$USERNAME/$DBPASSWORD@$DBNAME control=$CTLDIR/BMD_SZXA.ctl data=$FILEDIR/$sabfname log=$FILEDIR/sqlloader_$curdate.log
        mv $sabfname $BAKDIR
fi
if [ -f $sahfname ];then
        sqlldr userid=$USERNAME/$DBPASSWORD@$DBNAME control=$CTLDIR/HMD_SZXA.ctl data=$FILEDIR/$sahfname log=$FILEDIR/sqlloader_$curdate.log
        mv $sahfname $BAKDIR
fi

if [ -f $fsafname ];then
        sqlldr userid=$USERNAME/$DBPASSWORD@$DBNAME control=$CTLDIR/HMD_MonSZXA.ctl data=$FILEDIR/$fsafname log=$FILEDIR/sqlloader_$curdate.log
        mv $fsafname $BAKDIR
fi

sleep 180