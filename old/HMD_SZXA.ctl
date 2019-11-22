 
load data
infile *
append into table ZG.BI_IMEI_CALL_LIST
FIELDS TERMINATED BY ','
TRAILING NULLCOLS(
PHONE_ID        ,
INTEGRATION_ID  ,
IMEI            ,
AMOUNT                          ":AMOUNT*100",
PRESENT_AMOUNT  ":PRESENT_AMOUNT*100",
VALID_DATE                      Date "yyyy-mm-dd HH24:Mi:SS"    ,                                                                   
EXPIRE_DATE     Date "yyyy-mm-dd HH24:Mi:SS",
PRODUCT_CODE            ,
PRODUCT_NAME            ,
PRODUCT_DESC            ,     
FLAG            CONSTANT 1,
BILL_CYCLE      "to_char(SYSDATE,'yyyymm')" ,
DEAL_STATUS     CONSTANT 0,
DEAL_RESULT                     ,
ORDER_NO                                ,
BOSS_SEQ                                ,
INPUT_DATE      SYSDATE ,
FINISH_DATE     ,                             
REMARK                                  ,
FILE_NAME                               "'HMD_SZXA_'||to_char(add_months(sysdate,-1),'yyyymm')||'.dat'"
)