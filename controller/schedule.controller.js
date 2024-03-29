const coreDB = require('qaq-core-db');
var oracledb = require("oracledb");
const coreUtil = require('qaq-core-util');
var async = require("async");
var request = require('request');
var https = require("https");
var urlExists = require('url-exists');
var replaceall = require("replaceall");
let AWS = require('aws-sdk');
var dateFormat = require('dateformat');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
var excel = require('excel4node');
const Client = require('ssh2-sftp-client');
const fetch = require('node-fetch');

exports.pendingBidAllocMailApi = function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let poolName = redirectParam.poolName || '';
    var paramJson={};  
    var outJson={};

    var processNme = req.body.processNme || '';
    var buyerYN = req.body.buyerYN || 'yes';
    if(processNme != ''){
        let query="select process_idn  "+
		    "from stock_process p "+
            "where co_idn = $1 and nme = '"+processNme+"'||'_approved' and p.stt = 1 ";

        let params = [];
        params = [coIdn];
        
        coreDB.executeTransSql(tpoolconn,query,params,{},function(error,result){
            if(error){
                outJson["status"]="FAIL";
                outJson["message"]=error.message;
                callback(null,outJson);
            }else{
                var len=result.rows.length;

                if(len!=0){
                    let  processIdn = result.rows[0].process_idn || "";
                    //console.log("processIdn",processIdn);
                    paramJson["coIdn"]=coIdn;
                    paramJson["userIdn"]=null;
                    paramJson["pvtConfigure"]='';
                    paramJson["source"]=source;
                    paramJson["processIdn"]=processIdn;
                    paramJson["logUsr"]='';
                    paramJson["buyerYN"]=buyerYN;
                    paramJson["poolName"]=poolName;
                    mailSend(tpoolconn,paramJson,function(error,mailDetails){
                        if(error){
                        // console.log(error);
                            outJson["result"]='';
                            outJson["status"]="FAIL";
                            outJson["message"]="Fail To Send pendingBidAllocMail!";
                            callback(null,outJson);
                        }else{         
                            callback(null,mailDetails);                                                
                        }
                    })
                }else{
                    outJson["result"]='';
                    outJson["status"]="FAIL";
                    outJson["message"]="Fail To Find Process Idn!";
                    callback(null,outJson);
                }
            }
        })
    }else if(processNme == ''){
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Process Name Parameter";
        callback(null,outJson);
    }   
}

exports.pendingBidRejectionMailApi =async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let poolName = redirectParam.poolName || '';
    var cachedUrl = require('qaq-core-util').cachedUrl;
    var paramJson={};  
    var outJson={};
    let resultFinal = {};
    let formNme = "mailWebRequestFormat";

    var processNme = req.body.processNme || '';
    var buyerYN = req.body.buyerYN || 'yes';
    if(processNme != ''){
        let resultView = [];
        let methodParam = {};
        methodParam["formName"]=formNme;
        methodParam["display_key"]="result";
        methodParam["nme"]="WEB_MAIL";
        methodParam["userIdn"]=null;
        methodParam["coIdn"]=coIdn;
        methodParam["db"]=tpoolconn;
        methodParam["userCtg"]="";
        let attrresult = await coreUtil.pageDisplay(methodParam);
        resultView=attrresult.attr || [];

        methodParam = {};
        methodParam["resultView"]=resultView;
        methodParam["source"]=source;
        methodParam["processNme"]=processNme;
        methodParam["coIdn"]=coIdn;
        let pktResult = await execGetPacketDetails(methodParam,tpoolconn);
        if(pktResult.status == 'SUCCESS'){
            let buyerList = pktResult["buyerList"];
            let packetDtlMap = pktResult["result"];
            let attrDisplayDtl = pktResult["attrDisplayDtl"];
            let buyerListLen = buyerList.length;
            let tileWisearrayExec = [];
            //console.log(buyerList)
            let dbmsDtldata = await coreUtil.getCache("dbms_"+coIdn,cachedUrl);
            let formatNme = processNme+"_rejected";
            //console.log(formatNme)
            for(let k=0;k<buyerListLen;k++){
                let buyer = buyerList[k];
                let packetDtl = packetDtlMap[buyer];

                let methodParams = {};
                methodParams["packetDtl"]=packetDtl;
                methodParams["source"]=source;
                methodParams["coIdn"]=coIdn;
                methodParams["attrDisplayDtl"]=attrDisplayDtl;
                methodParams["resultView"]=resultView;
                methodParams["formatNme"]=formatNme;
                methodParams["buyerYN"]=buyerYN;
                methodParams["logUsr"]="";
                methodParams["poolName"]=poolName;
                tileWisearrayExec.push(function(callback) { execMailSendRejection(methodParams,callback) });
            }

            async.parallel(tileWisearrayExec,function(err,result){
                if (err) {
                    console.log(err);
                    outJson["result"]=resultFinal;
                    outJson["status"]="FAIL";
                    outJson["message"]=err;
                    callback(null,outJson);
                }
                let resultlen=result.length || 0;
                let totalSucessMail=0;
                for (let r=0;r<resultlen;r++){
                    let localresult=result[r].result || {};
                    totalSucessMail=totalSucessMail+localresult["count"] || 0;
                }

                outJson["result"]=resultFinal;
                outJson["status"]="SUCCESS";
                outJson["message"]="Total Mail Count ="+buyerListLen+" ,Sucess ="+totalSucessMail+" ,Fail ="+(buyerListLen-totalSucessMail);
                callback(null,outJson);
            })
        }else{
            callback(null,pktResult);
        }           
    }else if(processNme == ''){
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Process Name Parameter";
        callback(null,outJson);
    }   
}

async function mailSend(tpoolconn,redirectParam,callback){
    var userIdn = redirectParam.userIdn; 
    var coIdn = redirectParam.coIdn;
    var pvtConfigure = redirectParam.pvtConfigure;
    var source = redirectParam.source;
    var processIdn = redirectParam.processIdn;
    var logUsr = redirectParam.logUsr;
    var buyerYN = redirectParam.buyerYN;
    let poolName = redirectParam.poolName;
    var cachedUrl = require('qaq-core-util').cachedUrl;
    let resultFinal = {};
    var params = [];
    var list = [];
    let outJson = {};

    if(processIdn != ''){
        var query="select distinct(a.transaction_sales_idn) from transaction_sales a,transaction_d_sales b  "+
            "where a.process_idn=$1 and a.co_idn=$2 and a.transaction_sales_idn=b.transaction_sales_idn "+
            "and b.status in ('IS','CF') "+
            "and a.stt=1 and a.addl_attr ->> 'mail_send'='P' and a.trns_ts::date = current_date order by a.transaction_sales_idn";
        params = [processIdn,coIdn];
        coreDB.executeTransSql(tpoolconn,query,params,{},async function(error,result){
            if(error){
                outJson["result"]=resultFinal;
                outJson["status"]="FAIL";
                outJson["message"]=error.message;
                callback(null,outJson);
            }else{
                let len=result.rows.length;

                if(len!=0){
                    let tileWisearrayExec = [];
                    let  productAttributeM = await coreUtil.getCache("productAttributeM_"+coIdn,cachedUrl);
                    let productAttributeDtl = await coreUtil.getCache("productAttributeDtl_"+coIdn,cachedUrl);
                    let dbmsDtldata = await coreUtil.getCache("dbms_"+coIdn,cachedUrl);
                    for(let i=0;i<len;i++){
                        let transactionIdn = result.rows[i].transaction_sales_idn || "";
                        list.push(transactionIdn);
                        //console.log("transactionIdn"+transactionIdn);
                        
                        let methodParamlocal={};
                        methodParamlocal["transactionIdn"]=transactionIdn;
                        methodParamlocal["userIdn"]=userIdn;
                        methodParamlocal["coIdn"]=coIdn;
                        methodParamlocal["pvtConfigure"]=pvtConfigure;
                        methodParamlocal["source"]=source;
                        methodParamlocal["logUsr"]=logUsr;
                        methodParamlocal["productAttributeM"]=productAttributeM;
                        methodParamlocal["productAttributeDtl"]=productAttributeDtl;
                        methodParamlocal["dbmsDtldata"]=dbmsDtldata;
                        methodParamlocal["buyerYN"]=buyerYN;
                        methodParamlocal["poolName"]=poolName;
                        tileWisearrayExec.push(function(callback) { execMailSendTransSale(methodParamlocal,callback);});
                    }
                    //console.log("hi");

                    async.parallel(tileWisearrayExec,function(err,asyncresult){
                        //console.log("resulth"+result);
                        if (err) {
                            console.log("error ",err);
                            outJson["result"]=resultFinal;
                            outJson["status"]="FAIL";
                            outJson["message"]=err;
                            callback(null,outJson);
                        }
                        //console.log("result"+result);
                        let resultlen=asyncresult.length || 0;
                        //console.log(resultlen);
                        let totalSucessMail=0;
                        for (let r=0;r<resultlen;r++){
                            let localresult=asyncresult[r].result || {};
                            //console.log(localresult);
                            totalSucessMail=totalSucessMail+localresult["count"] || 0;
                        }

                        outJson["result"]=resultFinal;
                        outJson["status"]="SUCCESS";
                        outJson["message"]="Total Mail Count ="+len+" ,Sucess ="+totalSucessMail+" ,Fail ="+(len-totalSucessMail);
                        callback(null,outJson);
                    })
                }else{
                    outJson["result"]=resultFinal;
                    outJson["status"]="SUCCESS";
                    outJson["message"]="Sorry no mail pending data found";
                    callback(null,outJson);
                }
            }
        })
    }else if(processIdn == ''){
        outJson["result"]=resultFinal;
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Process Idn Parameter";
        callback(null,outJson);
    }    
}

function execMailSendTransSale(paramJson,callback){
    let transactionIdn = paramJson.transactionIdn;
    let userIdn = paramJson.userIdn;
    let coIdn = paramJson.coIdn;
    let pvtConfigure = paramJson.pvtConfigure;
    let source = paramJson.source;
    let logUsr = paramJson.logUsr;
    let productAttributeM = paramJson.productAttributeM;
    let productAttributeDtl = paramJson.productAttributeDtl;
    let dbmsDtldata = paramJson.dbmsDtldata;
    let buyerYN = paramJson.buyerYN;
    let poolName = paramJson.poolName;
    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || '';
    let formNme = paramJson.formNme || '';
    let downloadExcel = paramJson.downloadExcel || 'N';
    let fileName = paramJson.fileName;
    let outJson = {};
    //console.log(pool)
    if(pool!=''){
        coreDB.getTransPoolConnect(pool,async function(error,connection){
            if(error){
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                let methodParamlocal={};
                methodParamlocal["transactionIdn"]=transactionIdn;
                methodParamlocal["userIdn"]=userIdn;
                methodParamlocal["coIdn"]=coIdn;
                methodParamlocal["pvtConfigure"]=pvtConfigure;
                methodParamlocal["source"]=source;
                methodParamlocal["logUsr"]=logUsr;
                methodParamlocal["productAttributeM"]=productAttributeM;
                methodParamlocal["productAttributeDtl"]=productAttributeDtl;
                methodParamlocal["dbmsDtldata"]=dbmsDtldata;
                methodParamlocal["buyerYN"]=buyerYN;
                methodParamlocal["formNme"] = formNme;
                methodParamlocal["fileName"] = fileName;
                methodParamlocal["downloadExcel"] = downloadExcel;
                methodParamlocal["userEmail"] = "Y";
                let mailDetails = await coreUtil.sendTransMail(methodParamlocal,connection);
                coreDB.doTransRelease(connection);
                outJson["result"]=mailDetails.result || {};
                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                callback(null,outJson);
            }
        });
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Fail To Get Conection!";
        callback(null,outJson);
    }
}

function execGetPacketDetails(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getPacketDetails(tpoolconn, methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getPacketDetails(connection,paramJson,callback) {
    var coIdn = paramJson.coIdn;
    let resultView = paramJson.resultView;
    let source = paramJson.source;
    let processNme = paramJson.processNme;
    let productAttributeM = paramJson.productAttributeM || null;
    let productAttributeDtl = paramJson.productAttributeDtl || null;
    let outJson = {};
    var map = {};
    let resultViewlen=resultView.length;
    let dataTypeFormatDtl={};
    let attrDisplayDtl={};
    let formulaAttr = [];
    var cachedUrl = require('qaq-core-util').cachedUrl;

    if(productAttributeM == null)
        productAttributeM = await coreUtil.getCache("productAttributeM_"+coIdn,cachedUrl);

    if(productAttributeM == null){
        outJson["status"]="FAIL";
        outJson["message"]="Fail to get Product Attribute";
        callback(null,outJson);
    }else{
        productAttributeM = JSON.parse(productAttributeM);

        for(let j=0;j<resultViewlen;j++){
            let attr=resultView[j];
            let data_typ=productAttributeM[attr+"#T"] || '';
            let display=productAttributeM[attr+"#DS"] || '';
            let web=productAttributeM[attr+"#W"] || '';
            let format=productAttributeM[attr+"#F"] || '';
            let formula=productAttributeM[attr+"#FL"] || '';
            dataTypeFormatDtl[attr]=data_typ;
            dataTypeFormatDtl[attr+"#F"]=format;
            dataTypeFormatDtl[attr+"#FL"]=formula;
            if(data_typ=='f'){
                formulaAttr.push(attr);
                if(formula!='')
                dataTypeFormatDtl[attr+"#FL#ATTR"]=coreUtil.getDependAttrList(formula);
            }
            attrDisplayDtl[attr]=display;
        }
        outJson["attrDisplayDtl"]=attrDisplayDtl;

        if(productAttributeDtl == null)
             productAttributeDtl = await coreUtil.getCache("productAttributeDtl_"+coIdn,cachedUrl);
        
        if(productAttributeDtl == null){
            outJson["status"]="FAIL";
            outJson["message"]="Fail to get Product Sub Attribute";
            callback(null,outJson);
        }else{
            productAttributeDtl = JSON.parse(productAttributeDtl);
            let fmt = {};
            let params=[];
            var sql="select a.process_nme ,sp.print "+
            ", s.pkt_code,s.stock_type ,b.alloc_d_idn "+
            ", b.ignore_yn ,s.attr->>'certno' certno "+
            ", case when b.ignore_yn = 'yes' then a.process_nme||'_ignore' else a.process_nme||'_reject' end as mail_fmt "+
            ",b.transaction_web_idn,get_nme(c.nme_idn) buyer,c.nme_idn buyer_idn, "+
            "b.buyer_terms_idn buyerTermsIdn ,b.stock_idn,b.req_rte,b.req_dis, "+
            "(b.req_rte * s.weight_on_hand) req_amount , "+
            "to_char(a.created_ts,'dd-mm-yyyy') memodate , "+
            "to_char(a.created_ts + interval '5.5 hours', 'hh24:MI:SS') memotime ";
            for (let i = 0; i < resultViewlen; i++){
                let attr= resultView[i];
                if (attr == 'crtwt')
                    sql += ", trunc(CAST(attr ->> 'crtwt' as Numeric),2)  " + attr;
                else
                    sql+=", attr ->> '"+attr+"' "+attr;
            }
            sql+=" from bid_alloc a,bid_alloc_d b,buyer_terms c, stock_m s ,stock_process sp "+
                "where a.alloc_idn=b.alloc_idn and a.process_nme=$1 and a.co_idn=$2 "+
                "and b.stock_idn = s.stock_idn and s.co_idn=$3 "+
                "  and sp.nme=a.process_nme and sp.co_idn=$4 and sp.stt=1 "+
                "and c.buyer_terms_idn= b.buyer_terms_idn  "+
                "and b.alloc_status = 0 and (b.alloc='rej' or ignore_yn = 'yes') and COALESCE(b.addl_attr ->> 'mail_send','P') = 'P' and a.created_ts::date = current_date "+  
                "order by buyer,s.sort ";                      

            params.push(processNme);
            params.push(coIdn);
            params.push(coIdn);
            params.push(coIdn);
            //console.log(sql)
            //console.log(params)
            coreDB.executeTransSql(connection,sql,params,fmt,function(error,result){
                if(error){
                    console.log(error);
                    outJson["status"]="FAIL";
                    outJson["message"]="Error In getRejectPacketDetails Method!";
                    callback(null,outJson);
                }else{
                    var len=result.rows.length;
                    var buyerList = [];
                    if(len>0){
                        var prvBuyer = '';
                        var dataMap = [];
                        for(let i =0 ;i<len;i++){                                   
                            let data = result.rows[i];
                            var buyer = data.buyer;

                            if(prvBuyer == '')
                                prvBuyer = buyer;
                            
                            if(prvBuyer != buyer){
                                map[prvBuyer]=dataMap;
                                buyerList.push(prvBuyer);
                                prvBuyer = buyer;
                                dataMap = [];
                            }

                            var k = {};
                            k["process_nme"]=data.print;
                            k["transaction_web_idn"]=data.transaction_web_idn;
                            k["buyer_idn"]=data.buyer_idn;
                            k["buyerTermsIdn"]=data.buyertermsidn;
                            k["memodate"]=data.memodate;
                            k["memotime"]=data.memotime;

                            k["alloc_d_idn"]=data.alloc_d_idn;
                            k["packet"]=data.pkt_code;
                            k["certno"]=data.certno;
                            k["req_dis"]=data.req_dis || 0; 
                            let req_rte =data.req_rte;
                            req_rte=coreUtil.floorFigure(req_rte,2);
                            k["req_rte"]=req_rte;                                
                            let req_amount =data.req_amount;
                            req_amount=coreUtil.floorFigure(req_amount,2);
                            k["req_amount"]=req_amount;
                            k["stock_status"]=data.stock_status;                               
                            let stock_type=data.stock_type;
                            
                            for(let j=0;j<resultViewlen;j++){
                                let attr=resultView[j];
                                let data_typ=dataTypeFormatDtl[attr] || '';
                                let attrVal=data[attr];
                                if(data_typ=='c' && attrVal!='' && attrVal!=0){
                                    let prpSort=productAttributeDtl[attr+"#S"] || [];
                                    let prpDisplay=productAttributeDtl[attr+"#P"] || [];
                                    let prpWeb=productAttributeDtl[attr+"#W"] || [];
                                    if(source=='qsol' || source=='qs'){
                                        attrVal=prpDisplay[prpSort.indexOf(parseInt(attrVal))];
                                    }else{
                                        attrVal=prpWeb[prpSort.indexOf(parseInt(attrVal))];
                                    }
                                }else if(data_typ=='d' && attrVal!='' && attrVal!=0){
                                    attrVal=coreUtil.getDateyyyymmddToExpected(attrVal,'dd-mmm-yyyy');
                                }else if(data_typ=='n'&& attrVal!='' && attrVal!=0){
                                    let format=dataTypeFormatDtl[attr+"#F"] || 'N';
                                    if(format!='N'){
                                        if(stock_type!='NR' && attr=="crtwt"){
                                            format=3;
                                        }
                                        attrVal=attrVal || 0;
                                        attrVal=coreUtil.floorFigure(attrVal,format);
                                    }
                                }
                                k[attr]=attrVal;
                            }
                            dataMap.push(k);                                      
                        }
                        map[prvBuyer]=dataMap;
                        buyerList.push(prvBuyer);

                        outJson["status"]="SUCCESS";
                        outJson["message"]="SUCCESS";
                        outJson["result"]=map;
                        outJson["buyerList"]=buyerList;
                        callback(null,outJson);
                    }else{
                        outJson["status"]="FAIL";
                        outJson["message"]="Sorry result not found";
                        outJson["result"]=map;
                        callback(null,outJson);
                    }
                }
            })
        }  
    }
}

function execMailSendRejection(paramJson,callback){
    let packetDtl = paramJson.packetDtl;
    let source = paramJson.source;
    let coIdn = paramJson.coIdn;
    let attrDisplayDtl = paramJson.attrDisplayDtl;
    let resultView = paramJson.resultView;
    let formatNme = paramJson.formatNme;
    let buyerYN = paramJson.buyerYN;
    let logUsr = paramJson.logUsr;
    let poolName = paramJson.poolName;
    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || '';
    let outJson = {};
    //console.log(pool)
    if(pool!=''){
        coreDB.getTransPoolConnect(pool,async function(error,connection){
            if(error){
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                let methodParams = {};
                methodParams["packetDtl"]=packetDtl;
                methodParams["source"]=source;
                methodParams["coIdn"]=coIdn;
                methodParams["attrDisplayDtl"]=attrDisplayDtl;
                methodParams["resultView"]=resultView;
                methodParams["formatNme"]=formatNme;
                methodParams["buyerYN"]=buyerYN;
                methodParams["logUsr"]=logUsr;
                methodParams["userEmail"] = 'Y';
                let mailDetails = await coreUtil.sendRejectionMail(methodParams,connection);
                coreDB.doTransRelease(connection);
                outJson["result"]=mailDetails.result || {};
                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                callback(null,outJson);
            }
        })
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Fail To Get Conection!";
        callback(null,outJson);
    }
}

exports.saveMFGData = function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    var outJson={};

    let mfgPoolName = req.body.mfgPoolName || 'MFGPOOL';
    var fromDate = req.body.fromDate || '';
    var toDate = req.body.toDate || '';
    var fromDays = req.body.fromDays || '1';
    var toDays = req.body.toDays || '1';

    let methodParam = {};
    methodParam["mfgPoolName"] = mfgPoolName;
    methodParam["source"] = source;
    methodParam["coIdn"] = coIdn;
    methodParam["fromDate"] = fromDate;
    methodParam["toDate"] = toDate;
    methodParam["fromDays"] = fromDays;
    methodParam["toDays"] = toDays;
    methodParam["log_idn"] = log_idn;
    methodParam["poolName"] = poolName;
    let mfgResult = execSaveMFGDetails(methodParam);

    outJson["status"]="SUCCESS";
    outJson["message"]="MFG data inserted successfully";
    callback(null,outJson);          
}

function execSaveMFGDetails(methodParam) {
    return new Promise(function (resolve, reject) {
        saveMFGDetails( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function saveMFGDetails(redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source;
    let log_idn = redirectParam.log_idn;
    let mfgPoolName = redirectParam.mfgPoolName || 'MFGPOOL';
    var fromDate = redirectParam.fromDate || '';
    var toDate = redirectParam.toDate || '';
    var fromDays = redirectParam.fromDays || '1';
    var toDays = redirectParam.toDays || '1';
    let poolName = redirectParam.poolName;
    var paramJson={};  
    var outJson={};
    let resultFinal = {};
    let dtl = {};

    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || 'TPOOL';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,async function(error,tpoolconn){
            if(error){
                console.log(error);
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                let methodParam = {};
                methodParam["coIdn"] = coIdn;
                let result = await execDeleteMFGPlanPkt(methodParam,tpoolconn);
                
                methodParam = {};
                methodParam["mfgPoolName"] = mfgPoolName;
                methodParam["source"] = source;
                methodParam["coIdn"] = coIdn;
                methodParam["fromDate"] = fromDate;
                methodParam["toDate"] = toDate;
                methodParam["fromDays"] = fromDays;
                methodParam["toDays"] = toDays;
                let mfgResult = await execGetMFGDetails(methodParam);
                let mfgDataList = mfgResult["result"];
                dtl["getMFGDataStatus"] =  mfgResult.status;
                dtl["getMFGDataMessage"] =  mfgResult.message;
                dtl["getMFGDataCount"] = mfgDataList.length;
                let methodParams = {};
                methodParams["logDetails"] = dtl;
                methodParams["log_idn"] = log_idn;
                let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);
                if(mfgResult.status == 'SUCCESS'){
                    let attrIdList = mfgResult["attrIdList"];
                    //console.log("mfgDataList",mfgDataList.length)

                    methodParam = {};
                    methodParam["mfgDataList"] = mfgDataList;
                    methodParam["coIdn"] = coIdn;
                    let pktDataResult = await execInsertMFGPlanPkt(methodParam,tpoolconn);
                    if(pktDataResult.status == 'SUCCESS'){

                        methodParam = {};
                        methodParam["attrIdList"] = attrIdList;
                        methodParam["coIdn"] = coIdn;
                        let updatePktDataResult = await execUpdateMFGPlanPkt(methodParam,tpoolconn);
                        //if(updatePktDataResult.status == 'SUCCESS'){

                            //methodParam = {};
                        // methodParam["dtl"] = dtl;
                            //methodParam["coIdn"] = coIdn;
                        // methodParam["log_idn"] = log_idn;
                            //let updateMfgDataResult = await execUpdateMFGPlanPktFromStockM(methodParam,tpoolconn);
                            coreDB.doTransRelease(tpoolconn);
                            outJson["status"]="SUCCESS";
                            outJson["message"]="MFG data inserted successfully";
                            callback(null,outJson);
                        //}
                    } else {
                    // methodParam = {};
                    // methodParam["dtl"] = dtl;
                    // methodParam["coIdn"] = coIdn;
                    //  methodParam["log_idn"] = log_idn;
                    //  let updateMfgDataResult = await execUpdateMFGPlanPktFromStockM(methodParam,tpoolconn);
                        coreDB.doTransRelease(tpoolconn);
                        callback(null,pktDataResult);
                    }
                }else{
                // methodParam = {};
                // methodParam["dtl"] = dtl;
                /// methodParam["coIdn"] = coIdn;
                // methodParam["log_idn"] = log_idn;
                /// let updateMfgDataResult = await execUpdateMFGPlanPktFromStockM(methodParam,tpoolconn);
                    coreDB.doTransRelease(tpoolconn);
                    callback(null,mfgResult);
                }  
            }
        })
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }            
}

function execGetMFGDetails(methodParam) {
    return new Promise(function (resolve, reject) {
        getMFGDetails( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getMFGDetails(paramJson,callback) {
    var coIdn = paramJson.coIdn;
    let mfgPoolName = paramJson.mfgPoolName;
    let source = paramJson.source;
    let fromDate = paramJson.fromDate;
    let toDate = paramJson.toDate;
    let fromDays = paramJson.fromDays;
    let toDays = paramJson.toDays;
    let outJson = {};
    var mfgDataList = [];
    var poolsList= require('qaq-core-db').poolsList;
    mfgPoolName = mfgPoolName.trim();
    //console.log("mfgPoolName",mfgPoolName);
    var pool = poolsList[mfgPoolName] || '';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,function(error,mfgconnection){
            if(error){  
                console.log(error);
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get MFG Conection!";
                callback(null,outJson);   
            }else{
                let fmt = {};
                let params=[];
 
                var sql="with base_qry as \n"+
                    "( select c.lt_nmbr,c.asrt_pckt_nmbr,c.ref_nmbr pckt_nmbr,d.pln_id,d.pln_sqnc_nmbr ,c.pckt_id, \n"+
                    "d.attr_id \n"+
                    ", case when d.attr_id is null then c.pckt_cnt \n"+
                    "else case when c.pckt_nmbr=c.asrt_pckt_nmbr then c.pckt_cnt else 0 end end as rgh_wt \n"+
                    ", c.rgh_cnt as rgh_crts \n"+
                    ", c.stg_flg \n"+
                    ",coalesce(c.to_cmpn_cd,c .fctr_id) unit_id \n"+
                    ", (select p.pckt_id from pkt_master_m p where p.lt_nmbr=c.lt_nmbr and p.pckt_nmbr=c.asrt_pckt_nmbr) apkt_id, \n"+
                    "c.fnl_cnts cur_crts, coalesce(c.prcs_cd,c.nxt_prcs) prcs_cd, coalesce(c.fnct_cd,c.nxt_fnct) fnct_cd, \n"+
                    "c.pckt_stts, \n"+
                    "Rank() over(partition by c.pckt_id order by d.pln_id,d.pln_sqnc_nmbr) pln_sr, \n"+
                    "d.pln_vlu, \n"+
                    "e.unit_id mfg_fctr_id, \n"+
                    "e.blck_cd, \n"+
                    "(select p.prfn_nm from mfg_rule_m p \n"+
                    "where p.rule_typ='GM_FLW_MGR' and p.fctr_id=e.fctr_id \n"+
                    "and p.unit_id=e.unit_id \n"+
                    "and p.flg1=c.flw_typ \n"+
                    "--and p.flg3=e.blck_cd \n"+
                    "and p.to_dt is null \n"+
                    "limit 1) mfg_fctr_nm \n"+
                    "from pkt_master_m c \n"+
                    "left outer join mfg_pktcls_v e \n"+
                    "on e.pckt_id=c.pckt_id and e.stg_flg='GM' \n"+
                    "left outer join pkt_plning_t d \n"+
                    "on c.pckt_id=d.pckt_id \n"+
                    "and d.pln_id = ( select max(b.pln_id) from pkt_fnlpln_t b \n"+
                    "where b.pckt_id=c.pckt_id \n"+
                    "and b.trns_srno=(select max(a.trns_srno) from pkt_fnlpln_t a \n"+
                    "where a.pckt_id=b.pckt_id \n"+
                    "and a.pln_typ in ('MF','F') ))  \n"+
                    "where c.actv_flg='Y' and c.stg_flg not like 'RE%' \n"+
                    "--and c.lt_nmbr = 'LRYLGV' \n"+
                    ")  \n"+
                    "select  a.*  \n"+
                    ", case when a.pln_sr=1 then a.rgh_wt else 0 end rgh_wt \n"+
                    ", case when a.pln_sr=1 then a.rgh_crts else 0 end rgh_crts_nw \n"+
                    ",  (case when a.attr_id is not null then (select json_object_agg(lower(t.mprp), t.srt) from pkt_atrdtl_t t  \n"+
                    "where t.pckt_id=a.pckt_id and t.attr_id=a.attr_id and t.srt is not null) else null end ) attr  \n"+
                    "--case when a.pln_sr=1 then a.rgh_crts else 0 end rgh_crts_nw \n"+
                    "from base_qry a  \n"+
                    "where 1 = 1 --attr_id is not null   ";


                //params.push(fromDate);
                //params.push(toDate);
                //console.log(sql)
                //console.log(params)
                coreDB.executeTransSql(mfgconnection,sql,params,fmt,function(error,result){
                    if(error){
                        console.log("MFG",error);
                        coreDB.doRelease(mfgconnection);
                        outJson["status"]="FAIL";
                        outJson["message"]="Error In getMFGData Method!";
                        callback(null,outJson);
                    }else{
                        var len=result.rows.length;
                        //console.log("len",len);
                        let attrIdList = [];
                        if(len>0){
                            for(let i =0 ;i<len;i++){                                   
                                let data = result.rows[i];
                                var k = {};
                                k["mfg_pckt_id"]=data.pckt_id;
                                k["ase_pckt_id"]=data.apkt_id; 
                                let attr_id = data.attr_id; 
                                attrIdList.push(parseInt(attr_id));
                                k["attr_id"]= attr_id;
                                let lot = data.lt_nmbr;  
                                k["mfg_lt_nmbr"]=lot;
                                k["pln_id"]=data.pln_id;  
                                k["pln_sqnc_nmbr"]=data.pln_sqnc_nmbr; 
                                k["cstm_nmbr"]=data.pln_sr;
                                let rough_cts = data.rgh_crts_nw;
                                k["rgh_cts"] = rough_cts; 
                                let prcs_cd = data.prcs_cd;
                                let fnct_cd = data.fnct_cd;
                                let attr = data.attr || {}; 
                                attr["lot"] = lot;
                                attr["rough_cts"] = rough_cts;
                                attr["prcs_cd"] = prcs_cd;
                                attr["fnct_cd"] = fnct_cd;
                                k["attr"]=attr;  
                                k["mfg_pckt_nmbr"]=data.pckt_nmbr; 
                               // k["mfg_trns_dte"] = data.trns_dt || '';
                                k["mfg_stage"] = data.stg_flg;
                                k["plan_vlu"]=data.pln_vlu || 0;
                                k["mfg_fctr_nm"] = data.mfg_fctr_nm || '';
                                k["blck_cd"] = data.blck_cd || '';
                                k["rgh_wt"] = data.rgh_wt;
                                mfgDataList.push(k);                                      
                            }

                            //console.log("mfgDataListLen",mfgDataList);
                            coreDB.doRelease(mfgconnection);
                            outJson["status"]="SUCCESS";
                            outJson["message"]="SUCCESS";
                            outJson["result"]=mfgDataList;
                            outJson["attrIdList"]=attrIdList;
                            callback(null,outJson);
                        }else{
                            coreDB.doRelease(mfgconnection);
                            outJson["status"]="FAIL";
                            outJson["message"]="Sorry result not found";
                            outJson["result"]=mfgDataList;
                            callback(null,outJson);
                        }
                    }
                })
            }
        })
    } else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }
}

async function getMFGDetailsOld(paramJson,callback) {
    var coIdn = paramJson.coIdn;
    let mfgPoolName = paramJson.mfgPoolName;
    let source = paramJson.source;
    let fromDate = paramJson.fromDate;
    let toDate = paramJson.toDate;
    let fromDays = paramJson.fromDays;
    let toDays = paramJson.toDays;
    let outJson = {};
    var mfgDataList = [];
    var poolsList= require('qaq-core-db').poolsList;
    mfgPoolName = mfgPoolName.trim();
    //console.log("mfgPoolName",mfgPoolName);
    var pool = poolsList[mfgPoolName] || '';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,function(error,mfgconnection){
            if(error){  
                console.log(error);
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get MFG Conection!";
                callback(null,outJson);   
            }else{

                let fmt = {};
                let params=[];

                var sql="with planD as ( \n"+
                    "select \n"+
                    "a.lt_nmbr mfg_lt_nmbr,a.asrt_pckt_nmbr mfg_pckt_nmbr, \n"+
                    "(case when a.asrt_pckt_nmbr=a.pckt_nmbr then '0' else 1 end)::varchar cstm_nmbr, \n"+
                    "e.pckt_id mfg_pckt_id, \n"+
                    "f.lt_nmbr ase_lt_nmbr,f.pckt_nmbr ase_pckt_nmbr, \n"+
                    "f.pckt_id ase_pckt_id,d.pln_id,d.pln_sqnc_nmbr, \n"+
                    "b.trns_srno ase_trns_srno, \n"+
                    "b.rgh_cnt, b.cnts cur_crts, \n"+
                    "b.rtrn_dt::date trns_dt, \n"+
                    "D.attr_id,D.pln_vlu, \n"+
                    "h.prcn_cd ownr_plnr, \n"+
                    "g.empl_snm,(g.empl_fnm||' '||coalesce(g.empl_mnm,'')||' '||coalesce(g.empl_lnm,''))::varchar empl_nm, \n"+
                    "a.pckt_id mfg_sub_pckt_id \n"+
                    "from pkt_master_m a, pkt_issdtl_t b,pkt_retdtl_t c , pkt_plning_t d, pkt_master_m e,pkt_master_m f, \n"+
                    "per_employ_m G, \n"+
                    "pkt_issdtl_t h \n"+
                    "where date_trunc('day',b.rtrn_dt) between current_date - "+fromDays+" and current_date - "+toDays+" \n"+
                    "and \n"+
                    "b.pckt_id=a.pckt_id \n"+
                    "and b.pckt_id=c.pckt_id \n"+
                    "and b.trns_srno=c.trns_srno \n"+
                    "and b.prcs_cd='AS' \n"+
                    "and b.fnct_cd='MCK' \n"+
                    "and c.rtrn_typ='OK' \n"+
                    "and d.pckt_id=a.pckt_id \n"+
                    "and d.pln_id=c.pln_id \n"+
                    "and e.lt_nmbr=a.lt_nmbr \n"+
                    "and e.pckt_nmbr=a.asrt_pckt_nmbr \n"+
                    "and f.pckt_id=e.ref_pckt_id \n"+
                    "and g.empl_cd=h.prcn_cd \n"+
                    "and a.pckt_id=h.pckt_id \n"+
                    "and (b.trns_srno-1) = h.trns_srno \n"+
                    "and a.actv_flg='Y' \n"+
                    "--and a.pckt_id=5049265 \n"+
                    "and b.trns_Srno =(select min(z.trns_Srno) from pkt_issdtl_t z, pkt_retdtl_t y \n"+
                    "where z.pckt_id=a.pckt_id \n"+
                    "and z.pckt_id=y.pckt_id \n"+
                    "and z.trns_srno=y.trns_Srno \n"+
                    "and z.prcs_cd='AS' and z.fnct_cd='MCK' \n"+
                    "and y.rtrn_typ='OK') \n"+
                    ") \n"+
                    "--select row_to_json( \n"+
                    "select p.trns_dt, p.mfg_pckt_id, p.ase_pckt_id, p.mfg_pckt_nmbr, p.attr_id, p.ase_lt_nmbr, \n"+
                    "p.pln_id, p.pln_sqnc_nmbr, p.pln_vlu \n"+
                    ", p.cstm_nmbr \n"+
                    ", case when cast(p.cstm_nmbr as int) = 0 then p.rgh_cnt else 0 end \n"+
                    "as rgh_cts ,p.mfg_sub_pckt_id, \n"+
                    "json_object_agg(lower(t.mprp), t.srt) attr \n"+
                    "from pkt_atrdtl_t t, planD p \n"+
                    "where 1 = 1 \n"+
                    "and p.mfg_sub_pckt_id = t.pckt_id \n"+
                    "and p.attr_id= t.attr_id \n"+
                    "and t.srt is not null \n"+
                    "group by p.trns_dt, p.mfg_pckt_id, p.ase_pckt_id, p.mfg_pckt_nmbr,p.attr_id, p.ase_lt_nmbr, \n"+
                    "p.cstm_nmbr, p.pln_id, p.pln_sqnc_nmbr, p.pln_vlu, p.rgh_cnt,p.mfg_sub_pckt_id \n"+
                    "--) j ";

                //params.push(fromDate);
                //params.push(toDate);
                //console.log(sql)
                //console.log(params)
                coreDB.executeTransSql(mfgconnection,sql,params,fmt,function(error,result){
                    if(error){
                        console.log(error);
                        coreDB.doRelease(mfgconnection);
                        outJson["status"]="FAIL";
                        outJson["message"]="Error In getMFGData Method!";
                        callback(null,outJson);
                    }else{
                        var len=result.rows.length;
                        //console.log("len",len);
                        let attrIdList = [];
                        if(len>0){
                            for(let i =0 ;i<len;i++){                                   
                                let data = result.rows[i];
                                var k = {};
                                k["mfg_pckt_id"]=data.mfg_sub_pckt_id; // pckt_id
                                k["ase_pckt_id"]=data.ase_pckt_id; // apkt_id
                                let attr_id = data.attr_id; // attr_id
                                attrIdList.push(parseInt(attr_id));
                                k["attr_id"]= attr_id;
                                let lot = data.ase_lt_nmbr; //  lt_nmbr
                                k["mfg_lt_nmbr"]=lot;
                                k["pln_id"]=data.pln_id;  //pln_id
                                k["pln_sqnc_nmbr"]=data.pln_sqnc_nmbr; //  pln_sqnc_nmbr
                                k["cstm_nmbr"]=data.cstm_nmbr;
                                let rough_cts = data.rgh_cts;
                                k["rgh_cts"] = rough_cts; //rgh_crts
                                let attr = data.attr || {}; // attr
                                attr["lot"] = lot;
                                attr["rough_cts"] = rough_cts;
                                k["attr"]=attr;
                                k["plan_vlu"]=data.pln_vlu;
                                k["mfg_pckt_nmbr"]=data.mfg_pckt_nmbr; //pckt_nmbr
                                k["mfg_trns_dte"] = data.trns_dt;
                                k["mfg_stage"] = "MKB";
                                mfgDataList.push(k);                                      
                            }

                            //console.log("mfgDataListLen",mfgDataList.length);
                            coreDB.doRelease(mfgconnection);
                            outJson["status"]="SUCCESS";
                            outJson["message"]="SUCCESS";
                            outJson["result"]=mfgDataList;
                            outJson["attrIdList"]=attrIdList;
                            callback(null,outJson);
                        }else{
                            coreDB.doRelease(mfgconnection);
                            outJson["status"]="FAIL";
                            outJson["message"]="Sorry result not found";
                            outJson["result"]=mfgDataList;
                            callback(null,outJson);
                        }
                    }
                })
            }
        })
    } else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }
}

function execDeleteMFGPlanPkt(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        deleteMFGPlanPkt(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function deleteMFGPlanPkt(methodParam,tpoolconn,callback){
    var coIdn = methodParam.coIdn;
    let params=[];
    let fmt = {};
    let outJson = {};
   
    let sql=" truncate table mfg_plan_pkt_t"; 

    //console.log(sql);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn,sql,params,fmt,function(error,result){
        if(error){
            console.log(error)
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In mfg_plan_pkt_t Method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            //console.log("rowCount",rowCount)
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson);       
        }
    });
}

function execInsertMFGPlanPkt(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        insertMFGPlanPkt(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function insertMFGPlanPkt(methodParam,tpoolconn,callback){
    var mfgDataList = methodParam.mfgDataList;
    var coIdn = methodParam.coIdn;
    let params=[];
    let fmt = {};
    let outJson = {};

   
    let insertQ="insert into mfg_plan_pkt_t(mfg_pckt_id,ase_pckt_id,"+
        "attr_id,mfg_lt_nmbr,pln_id,pln_sqnc_nmbr,cstm_nmbr,attr,rgh_cts ,mfg_pckt_nmbr,mfg_stage,plan_vlu,mfg_fctr_nm,blck_cd,rgh_wt,stt,created_ts) "+ //,mfg_trns_dte
        "select *,1 stt,current_timestamp created_ts from "+
        "jsonb_to_recordset('"+JSON.stringify(mfgDataList)+"'::jsonb) "+
        "as x(mfg_pckt_id int,ase_pckt_id int,attr_id bigInt,mfg_lt_nmbr varchar,"+
        "pln_id bigInt,pln_sqnc_nmbr int,cstm_nmbr int,attr jsonb,rgh_cts numeric,mfg_pckt_nmbr varchar,mfg_stage varchar,plan_vlu numeric,mfg_fctr_nm varchar,blck_cd varchar,rgh_wt numeric)  "; //,mfg_trns_dte date

    //console.log(insertQ);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn,insertQ,params,fmt,function(error,result){
        if(error){
            console.log(error)
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In mfg_plan_pkt_t Method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            //console.log("rowCount",rowCount);
            if(rowCount>0){
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="mfg_plan_pkt_t Inserted Failed!";
                callback(null,outJson);
            }    
        }
    });
}

function execUpdateMFGPlanPkt(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        updateMFGPlanPkt(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateMFGPlanPkt(methodParam,tpoolconn,callback){
    var attrIdList = methodParam.attrIdList;
    var coIdn = methodParam.coIdn;
    let params=[];
    let fmt = {};
    let outJson = {};
    //console.log("attrIdList",attrIdList)
   
    let sql="update mfg_plan_pkt_t set attr = "+
            "attr || concat('{\"sz\": ', COALESCE(get_sz($1,cast(attr ->> 'crtwt' as numeric), 'attr', 'sz'),'0') "+
            ",',\"gsz\":',COALESCE(get_sz($2,cast(attr ->> 'crtwt' as numeric), 'attr', 'gsz'),'0') ,'}')::jsonb "+
            "where created_ts::date = current_date ";
        params=[];
        params.push(coIdn);
        params.push(coIdn);
        //console.log(sql);
        //console.log(params);
    coreDB.executeTransSql(tpoolconn,sql,params,fmt,function(error,result){
        if(error){
            console.log(error)
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In update mfg_plan_pkt_t Method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            if(rowCount>0){ 
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);         
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="mfg_plan_pkt_t updation failed!";
                callback(null,outJson);
            }    
        }
    });
}

function execUpdateMFGPlanPktFromStockM(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        updateMFGPlanPktFromStockM(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateMFGPlanPktFromStockM(methodParam,tpoolconn,callback){
    var coIdn = methodParam.coIdn;
    let log_idn = methodParam.log_idn;
    let dtl = methodParam.dtl || {};
    let params=[];
    let fmt = {};
    let outJson = {};
    //console.log("attrIdList",attrIdList)
    
    //let sql="Update mfg_plan_pkt_t m set stt = 0, pkt_code = s.pkt_code  "+
    //    "from stock_m s , stock_status ss  "+
    //    "where m.stt = 1 "+
    //   "and s.co_idn = ss.co_idn and s.status = ss.status "+
    //    "and s.co_idn = $1 "+
    //    "and s.stock_type in ('NR','SMX') "+
    //    "and ss.bi_group in ('asrt','lab') "+
    //    "and m.lot_no = concat(s.attr ->> 'lot', '-', coalesce(s.attr ->> 'lot_map','0')) ";

    let sql = "update mfg_plan_pkt_t t set stt = 0 where stt = 1 and exists ( \n"+
         " select 1 from stock_m s where s.co_idn = $1 and s.pkt_code = t.mfg_pckt_id::text) ";    

    params=[];
    params.push(coIdn);
    console.log(sql);
    console.log(params);
    coreDB.executeTransSql(tpoolconn,sql,params,fmt,async function(error,result){
        if(error){
            console.log(error)
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In update mfg_plan_pkt_t Method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            dtl["mfgUpdateCount"] = rowCount;
            //let methodParams = {};
            //methodParams["logDetails"] = dtl;
            //methodParams["log_idn"] = log_idn;
            //let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);
            if(rowCount>0){
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="mfg_plan_pkt_t updation failed!";
                callback(null,outJson);
            } 
        }
    })        
}

function execUpdateScheduleLog(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        updateScheduleLog(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateScheduleLog(methodParam,tpoolconn,callback){
    var logDetails = methodParam.logDetails || {};
    var log_idn = methodParam.log_idn;
    let params=[];
    let fmt = {};
    let outJson = {};
    
    let updateQ="update schedule_log set log_attr = log_attr || '"+JSON.stringify(logDetails)+"' "+
            " where log_idn = $1 ";
    params = [];
    params.push(log_idn);
    //console.log(updateQ);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn,updateQ,params,fmt,function(error,result){
        if(error){
          
            outJson["status"]="FAIL";
            outJson["message"]="Error In updateScheduleLog Method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;   
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson);                     
        }
    });
}

exports.saveMFGCaratPrice = async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let formNme ="caratRateApiForm";
    let outJson = {};
    var resultFinal = {};
    let fmt = {};

    let methodParam = {};
    methodParam["formName"] = formNme;
    methodParam["display_key"] = "result";
    methodParam["nme"] = null;
    methodParam["coIdn"] = coIdn;
    methodParam["db"] = tpoolconn;
    let attrresult =await coreUtil.pageDisplay(methodParam);
    let resultView = attrresult.attr || [];

    methodParam = {};
    methodParam["resultView"] = resultView;
    methodParam["coIdn"] = coIdn;
    let packetDtl = await execGetMFGPacketDetails(methodParam,tpoolconn);
    if(packetDtl.status == 'SUCCESS'){

        let pktDtlList = packetDtl.result || [];
        let tileWisearrayExec = [];
        for(let m=0; m<pktDtlList.length; m++){
            let pktData = pktDtlList[m];
            let data = {};
            data["username"]="tech@kapugems.com";
            data["search_type"]="S";
            data["cert"]='GIA';
            let sh = pktData["sh"];
            data["shape"]= sh;
            data["carat"]=pktData["crtwt"];
            data["color"]=pktData["co"];
            data["clarity"]=pktData["pu"];
            data["cut"]=pktData["ct"];
            let pol = pktData["po"];
            let sy = pktData["sy"];
            data["pol_sym"]=pol;
            let fl = pktData["fl"];
            data["flr"]=fl;
            data["ratio_cut"]="";
            data["ratio"]=pktData["lw"];
            let paraMap = {};
            paraMap["girdle_p"]=pktData["gd_p"];
            paraMap["crn_height_p"]=pktData["ch"];
            paraMap["crn_ang"]=pktData["ca"];
            paraMap["pav_depth_p"]=pktData["pd"];
            paraMap["pav_ang"]=pktData["pa"];
            paraMap["table_p"]=pktData["tbl"];
            paraMap["total_depth_p"]=pktData["dp"];
            paraMap["length"]=pktData["max_mm"];
            paraMap["width"]=pktData["min_mm"];
            data["para"]=paraMap;
            let attr_id = pktData["attr_id"];
            console.log("data",data);
            let methodParamLocal = {};
            methodParamLocal["packetData"] = data;
            methodParamLocal["attr_id"] = attr_id;
            methodParamLocal["coIdn"] = coIdn;
            methodParamLocal["logUsr"] = "auto";
            //console.log(methodParamLocal)
            tileWisearrayExec.push(function (callback) { getAndUpdatePrice(methodParamLocal, tpoolconn, callback); });
        }
        async.parallel(tileWisearrayExec, function (err, result) {
            if (err) {
                console.log(err);
                outJson["message"]=err;
                outJson["status"]="FAIL";
                callback(null,outJson);  
            } else {   
                let resultlen = result.length || 0;
                let totalPackets = pktDtlList.length;
                let totalSucessPackets = 0;
                for (let r = 0; r < resultlen; r++) {
                    let localresult = result[r].result || {};
                    totalSucessPackets = totalSucessPackets + localresult["count"] || 0;
                }

                outJson["status"]="SUCCESS";
                outJson["message"]="Total Packets =" + totalPackets + " ,Success =" + totalSucessPackets + " ,Fail =" + (totalPackets - totalSucessPackets); 
                callback(null,outJson);  
            }
        })
    }else {
        callback(null,packetDtl);
    }
}

function execGetMFGPacketDetails (methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getMFGPacketDetails(methodParam,tpoolconn, function (error, result) {
            if(error){  
            reject(error);
            }
            resolve(result);
        });
    });
}

function getMFGPacketDetails(paramJson, connection, callback){
    let resultView = paramJson.resultView || [];
    let coIdn = paramJson.coIdn;
    let resultViewlength = resultView.length;
    let outJson = {};
    let summaryDtls = [];
    let dataTypeFormatDtl = {};

    var cachedUrl = require('qaq-core-util').cachedUrl;
    coreUtil.getCache("productAttributeM_" + coIdn, cachedUrl).then(productAttributeM => {
        if (productAttributeM == null) {
            outJson["status"] = "FAIL";
            outJson["message"] = "Fail to get Product Attribute";
            callback(null, outJson);
        } else {
            productAttributeM = JSON.parse(productAttributeM);

            coreUtil.getCache("productAttributeDtl_" + coIdn, cachedUrl).then(productAttributeDtl => {
                if (productAttributeDtl == null) {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Fail to get Product Sub Attribute";
                    callback(null, outJson);
                } else {
                    productAttributeDtl = JSON.parse(productAttributeDtl);

                    for (let j = 0; j < resultViewlength; j++) {
                        let attr = resultView[j];
                        let data_typ = productAttributeM[attr + "#T"] || '';
                        let format = productAttributeM[attr + "#F"] || '';
                        let formula = productAttributeM[attr + "#FL"] || '';
                        dataTypeFormatDtl[attr] = data_typ;
                        dataTypeFormatDtl[attr + "#F"] = format;
                    }
                    
                    let sql = "select attr_id  ";
                    for (let i = 0; i < resultViewlength; i++) {
                        let attr = resultView[i];
                        if (attr == 'crtwt')
                            sql += ", trunc(CAST(attr ->> 'crtwt' as Numeric),2)  " + attr;
                        else
                            sql += ", COALESCE(attr ->> '" + attr + "','') " + attr;
                    }
                    sql+=" from mfg_plan_pkt_t where "+
                        "created_ts::date = current_date and stt = 1 "+
                        " and attr_id = 5024834001 ";


                    let params = [];

                    //console.log(sql);
                    //console.log(params);
                    coreDB.executeTransSql(connection,sql,params,{},function(error,result){
                        if(error){
                            console.log(error)
                            outJson["status"]="FAIL";
                            outJson["message"]="Error In getPacketDetails Method!";
                            callback(null,outJson);
                        }else{
                            var len = result.rows.length;
                            //console.log("len",len);
                            if (len > 0) {
                                for (let i = 0; i < len; i++) {
                                    var resultRow = result.rows[i];
                                    //console.log(resultRow)
                                    let pktDtl = {};
                                    let stock_type = 'NR';
                                    pktDtl["attr_id"] = resultRow["attr_id"];
                                    for (let j = 0; j < resultViewlength; j++) {
                                        let attr = resultView[j];
                                        let data_typ = dataTypeFormatDtl[attr] || '';
                                        let attrVal = resultRow[attr];
                                        if (data_typ == 'c' && attrVal != '' && attrVal != 0) {
                                            let prpSort = productAttributeDtl[attr + "#S"] || [];
                                            let prpDisplay = productAttributeDtl[attr + "#P"] || [];
                                            let prpOutMappig = productAttributeDtl[attr + "#OM"] || [];
                                            let outMapObj =  prpOutMappig[prpSort.indexOf(parseInt(attrVal))];
                                            let displayVal = prpDisplay[prpSort.indexOf(parseInt(attrVal))];
                                            attrVal = outMapObj["cr"] || displayVal.toUpperCase();
                                        } else if (data_typ == 'd' && attrVal != '' && attrVal != 0) {
                                            attrVal = coreUtil.getDateyyyymmddToExpected(attrVal, 'dd-mmm-yyyy');
                                        } else if (data_typ == 'n' && attrVal != '' && attrVal != 0) {
                                            let format = dataTypeFormatDtl[attr + "#F"] || 'N';
                                            if (format != 'N') {
                                                if (stock_type != 'NR' && attr == "crtwt") {
                                                    format = 3;
                                                }
                                                attrVal = attrVal || 0;
                                                attrVal = coreUtil.floorFigure(attrVal, format);
                                            }
                                        }
                                        pktDtl[attr] = attrVal;
                                    }
                                    summaryDtls.push(pktDtl);
                                }
                                //console.log("data"+summaryDtls);

                                outJson["result"] = summaryDtls;
                                outJson["status"] = "SUCCESS";
                                outJson["message"] = "SUCCESS";
                                callback(null, outJson);
                            } else {
                                outJson["status"] = "SUCCESS";
                                outJson["message"] = "Sorry No Result Found";
                                callback(null, outJson);
                            }
                        }
                    })  
                }
            })
        }
    })  
}

function getAndUpdatePrice(paramJson, connection, callback){
    let packetData = paramJson.packetData || {};
    let attr_id = paramJson.attr_id || '';
    let coIdn = paramJson.coIdn;
    let logUsr = paramJson.logUsr;
    let outJson = {};
    //console.log("stockIdn",stockIdn);
    //console.log("packetData",JSON.stringify(packetData));
    let carateRate = '';
    let resultFinal = {};

    var headers = {
        'Content-Type':'application/json',
        'Accept':'application/json'
    }

    var options = {
        url: 'http://www.caratrate.com/app/search/input',
        method: 'POST',
        headers: headers,
        form: JSON.stringify(packetData)
    };
    request(options,async function (error, response, body) {
        //console.log(error);
        //console.log("statusCode"+response.statusCode );
        //console.log(response.status );
        //console.log(response.message );
        if (!error && response.statusCode == 200) {
            console.log("attr_id",attr_id);
            console.log("body"+body); // Print the shortened url.
            try {
                let info = JSON.parse(body);
                
                carateRate = info.ppc || ''; 
                if(carateRate != ''){
                    methodParam = {};
                    methodParam["carateRate"] = carateRate;
                    methodParam["logUsr"] = logUsr;
                    methodParam["coIdn"] = coIdn;
                    methodParam["attr_id"] = attr_id;
                    methodParam["packetData"] = packetData;
                    updatePrice(methodParam, connection,  function (error, priceDetails) {
                        if (error) {
                            console.log(error);
                            outJson["result"] = '';
                            outJson["status"] = "FAIL";
                            outJson["message"] = "Fail To Update Price in stock master!";
                            callback(null, outJson);
                        } else {
                            callback(null, priceDetails);
                        }
                    })
                } else{
                    console.log(error);
                    outJson["message"]=error;
                    outJson["status"]="FAIL";
                    callback(null,outJson);   
                }  
            } catch(e) {
                outJson["status"] = "FAIL";
                outJson["message"] = "Error in Carat Rate API";
                callback(null, outJson);
            }       
        }else{
            console.log(error);
            outJson["message"]=error;
            outJson["status"]="FAIL";
            callback(null,outJson);   
        }
    });   
}

async function updatePrice(paramJson, connection, callback){
    let carateRate = paramJson.carateRate || '';
    let logUsr = paramJson.logUsr || '';
    let coIdn = paramJson.coIdn;
    let attr_id = paramJson.attr_id;
    let attr = paramJson.packetData;
    let outJson = {};
    let resultFinal = {};
    let params = [];

    //let attrDtl = {};
    //attrDtl["cr_ppc"] = carateRate;

    methodParam = {};
    methodParam["coIdn"] = coIdn;
    methodParam["sh"] = attr["shape"];
    methodParam["crtwt"] = attr["carat"];
    methodParam["co"] = attr["color"];
    methodParam["pu"] = attr["clarity"];
    let rapRteResult = await coreUtil.execGetBenchMarkRte(methodParam,connection);
    if(rapRteResult.status == 'SUCCESS'){
        rapRte = rapRteResult.result || '1';
    } 
    rapRte = parseFloat(rapRte);

    let sql = "update mfg_plan_pkt_t set "+
    " attr = attr || concat('{\"cr_ppc\":', "+carateRate+", "+
    "',\"rap_rte\":', "+rapRte+", "+
    "',\"cr_dis\":', trunc(("+carateRate+"*100/"+rapRte+") - 100,2), '}')::jsonb "+
    " ,modified_ts=current_timestamp,modified_by=$1 "+
    " where "+
    " attr_id = $2 ";

    params.push(logUsr);
    params.push(attr_id);

    //console.log(sql);
    //console.log(params);
    coreDB.executeTransSql(connection,sql,params,{},function(error,result){
        if(error){
            console.log(error);
            outJson["status"]="FAIL";
            outJson["message"]="Error In updateStockM Method!";
            callback(null,outJson);
        }else{
            var len = result.rowCount;
            //console.log("len",len);
            if (len > 0) {
                resultFinal["count"] = 1;
                outJson["result"] = resultFinal;
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);          
            } else {
                outJson["status"] = "FAIL";
                outJson["message"] = "MfgPlanPkt not updated";
                callback(null, outJson);
            }
        }
    })
}

exports.updateExchangeRte =async function (req, res, connection, redirectParam, callback) {
    var coIdn = redirectParam.coIdn;
    var log_idn = redirectParam.log_idn;
    let source = redirectParam.source || req.body.source;
    let outJson = {};
    let paramJson = {};
    var resultFinal = {};
    var currency = 'INR';
    let exhRte = '';
    let methodParam={};
    let dtl = {};
    let currency_idn = '';

    let moneyXrtResult = await execGetMoneyConXrt(methodParam);
    if(moneyXrtResult.status == 'SUCCESS'){
        let data = moneyXrtResult["data"] || '';
        dtl["moneyControlXrt"] = data;
        data = coreUtil.floorFigure(data, 2);
        let xrt = parseFloat(data) || '';
        console.log("xrt",xrt);
        if(xrt == ''){
            moneyXrtResult = await execGetXrtNew(methodParam);
            if(moneyXrtResult.status == 'SUCCESS'){
                data = moneyXrtResult["data"] || '';
                dtl["moneyControlXrt"] = data;
                data = coreUtil.floorFigure(data, 2);
                xrt = parseFloat(data) || '';
                console.log("xrtNew",xrt);
            }
        }
        if(xrt != ''){
            methodParam={};
            methodParam["coIdn"]=coIdn;
            methodParam["currency"]=currency;            
            let pgXrtResult = await execGetPGCurrentXrt(methodParam,connection);
            if(pgXrtResult.status == 'SUCCESS'){
                let pgxrt = pgXrtResult["data"] || '';
                currency_idn = pgXrtResult["currency_idn"] || '';
                pgxrt = parseFloat(pgxrt) || '';
                console.log("pgxrt",pgxrt);
                if(xrt != '' && pgxrt != ''){
                    let xrt_diff = Math.abs(xrt - pgxrt);
                    xrt_diff = coreUtil.floorFigure(xrt_diff, 2);
                    console.log("xrt_diff",xrt_diff);
                    if(parseFloat(xrt_diff) < 1.50 )
                        exhRte = xrt;
                }
            } 
        } else {
            var param={};
            param["coIdn"]=coIdn;
            param["formatNme"]="updatexrt";
            param["status"]="FAIL";
            param["message"]="Money control exchange rate not found";
            let mailResult = await execSendXrtMail(param,connection); 
        }
    } else {
        var param={};
        param["coIdn"]=coIdn;
        param["formatNme"]="updatexrt";
        param["status"]="FAIL";
        param["message"]=moneyXrtResult.message;
        let mailResult = await execSendXrtMail(param,connection); 
    }   
    console.log("current exhRte",exhRte);
    dtl["updatedXrt"] = exhRte;
    let methodParams = {};
    methodParams["logDetails"] = dtl;
    methodParams["log_idn"] = log_idn;
    let logResult = await execUpdateScheduleLog(methodParams,connection);

    if (exhRte != '' && currency != '' && currency_idn != '') {
        dtl = {};
        methodParam={};
        methodParam["exhRte"]=exhRte;
        methodParam["coIdn"]=coIdn;
        methodParam["currency"]=currency; 
        methodParam["source"]=source;    
        methodParam["currency_idn"]= currency_idn;          
        let pgXrt = await execUpdatePGXrt(methodParam,connection);
        dtl["pgXrtResult"] = pgXrt;       
       // let oraXrt = await execUpdateOraXrt(methodParam);
        //dtl["oracleXrtResul"] = oraXrt;

        methodParams = {};
        methodParams["logDetails"] = dtl;
        methodParams["log_idn"] = log_idn;
        let logResult = await execUpdateScheduleLog(methodParams,connection);

        if(pgXrt.status== 'SUCCESS'){ //&& oraXrt.status=='SUCCESS'
            outJson["result"] = resultFinal;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "Exh Rte updated Successfully!";
            callback(null, outJson);
        }else if(pgXrt.status== 'FAIL')
            callback(null, pgXrt);
        //else if( oraXrt.status=='FAIL')
        //    callback(null, oraXrt);
    } else if (exhRte == '') { 
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify Exh Rte Can not be blank!";
        callback(null, outJson);
    } else if (currency == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify Currency Can not be blank!";
        callback(null, outJson);
    }  else if (currency_idn == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify Currency Idn Can not be blank!";
        callback(null, outJson);
    }
}

function execGetMoneyConXrt(methodParam){
    return new Promise(function(resolve,reject) {
        getMoneyConXrt(methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function getMoneyConXrt(methodParam,callback){
    let outJson = {};

    var url = "https://www.moneycontrol.com/currency/mcx-usdinr-price.html";

    https.get(url, (resp) => {
        let data = '';

        // A chunk of data has been recieved.
        resp.on('data', (chunk) => {
            data += chunk;
        });
        
        // The whole response has been received. Print out the result.
        resp.on('end', () => {
            let htmlData = data.toString();
            let fullPageArr = htmlData.split("r_20'><strong>");
            let valArr = fullPageArr[1] || '';
            let val = valArr.split("bse_img_top");
            let valFirst = val[0] || '';
            //console.log("valFirst",valFirst)
            let strong = valFirst.split("</strong>");
            let strongFirst = strong[0] || '';
            //strongFirst = replaceall("'><strong>","",strongFirst);
            console.log("MoneyControlXRT: ",strongFirst);
            outJson["data"] = strongFirst;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson);
        });

    }).on("error", (err) => {
        console.log("Error: " + err.message);
        outJson["status"]="FAIL";
        outJson["message"]="Error In get xrt from money control!"+err.message;
        callback(null,outJson);
    });
}

function execGetPGCurrentXrt(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getPGCurrentXrt(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
     });
    });
}

function getPGCurrentXrt(methodParam,tpoolconn,callback){
    var coIdn = methodParam.coIdn;
    var currency = methodParam.currency;
    let params=[];
    let fmt = {};
    let outJson = {};

   
    let updateQ="select a.xrte,a.currency_idn from currency_xrt a, "+
        " gen_currency b where a.currency_idn = b.currency_idn "+
        "and a.co_idn=$1 and a.stt=1 and a.end_dt is null and b.nme=$2";

    params.push(coIdn);
    params.push(currency);
    
    // console.log(updateQ);
    // console.log(params);
    coreDB.executeTransSql(tpoolconn,updateQ,params,fmt,function(error,result){
        if(error){
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In get current xrt method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            if(rowCount>0){
                var resultRow=result.rows[0] || {};
                var xrte = resultRow["xrte"];
                let currency_idn = resultRow["currency_idn"];
                //console.log(currencyIdn);
                outJson["data"] = xrte;
                outJson["currency_idn"] = currency_idn;
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);            
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="Get Xrt Method Failed!";
                callback(null,outJson);
            }    
        }
    });
}

function execUpdatePGXrt(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        updatePGXrt(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
     });
    });
}

function updatePGXrt(methodParam,tpoolconn,callback){
    var exhRte = methodParam.exhRte;
    var coIdn = methodParam.coIdn;
    var logUsr = methodParam.logUsr || '';
    var currency = methodParam.currency;
    let source = methodParam.source || 'api';
    let currencyIdn = methodParam.currency_idn || '';
    let params=[];
    let fmt = {};
    let outJson = {};

   
    let updateQ="update currency_xrt a set stt=0,end_dt=current_timestamp,modified_by=$1,modified_ts= current_timestamp "+
        "from gen_currency b where a.currency_idn = b.currency_idn "+
        "and a.co_idn=$2 and a.end_dt is null and b.nme=$3 ";

    params.push(logUsr);
    params.push(coIdn);
    params.push(currency);
    
    // console.log(updateQ);
    // console.log(params);
    coreDB.executeTransSql(tpoolconn,updateQ,params,fmt,function(error,result){
        if(error){
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In update currency_xrt method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            //if(rowCount>0){
                //console.log(currencyIdn); \\ if xrt updation fail that time also e have to add fresh entry for xrt

                var insertQ="insert into currency_xrt(co_idn,currency_idn,xrte,src,pvt,stt,created_ts,created_by) "+
                    "values($1,$2,$3,$4,$5,$6,current_timestamp,$7)";
    
                params =[];
                params.push(coIdn);
                params.push(currencyIdn);
                params.push(exhRte);
                params.push(source);
                params.push(0);
                params.push(1);
                params.push(logUsr);
                // console.log(insertQ);
                // console.log(params);
                coreDB.executeTransSql(tpoolconn,insertQ,params,fmt,async function(error,result){
                    if(error){
                        var param={};
                        param["coIdn"]=coIdn;
                        param["formatNme"]="updatexrt";
                        param["status"]="FAIL";
                        param["message"]="Error In insert currency_xrt Method!"+error.message;
                        let mailResult = await execSendXrtMail(param,tpoolconn); 
                        coreDB.doTransRollBack(tpoolconn);
                        outJson["status"]="FAIL";
                        outJson["message"]="Error In insert currency_xrt Method!"+error.message;
                        callback(null,outJson);
                    }else{
                        ///coreDB.doTransCommit(tpoolconn);
                        var rowCount = result.rowCount;
                        if(rowCount>0){     
                            //console.log("DONE");
                            outJson["status"] = "SUCCESS";
                            outJson["message"] = "SUCCESS";
                            callback(null, outJson);
                        }else{
                            var param={};
                            param["coIdn"]=coIdn;
                            param["formatNme"]="updatexrt";
                            param["status"]="FAIL";
                            param["message"]="CurrencyXrt Insertion Failed!";
                            let mailResult = await execSendXrtMail(param,tpoolconn); 
                            outJson["status"]="FAIL";
                            outJson["message"]="CurrencyXrt Insertion Failed!";
                            callback(null,outJson);
                        } 
                    }
                })
            //}else{
            //    outJson["status"]="FAIL";
            //    outJson["message"]="CurrencyXrt Updation Failed!";
            //    callback(null,outJson);
            //}    
        }
    });
}

function execUpdateOraXrt(methodParam){
    return new Promise(function(resolve,reject) {
        updateOraXrt(methodParam,function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
     });
    });
}

function updateOraXrt(methodParam,callback){
    var exhRte = methodParam.exhRte;
    var currency = methodParam.currency;
    let oraclefmt = {};
    let oracleparams = [];
    let outJson = {};

    coreDB.getPoolConnect("KGWPOOL",function(error,oracleconnection){
        if(error){    
            outJson["status"]="FAIL";
            outJson["message"]="Fail To Get Conection!";
            callback(null,outJson);   
        }else{  
            let updateQ="update lcl_xrt set to_dte = sysdate where to_dte is null and cur=:currency ";

            oracleparams={currency};
            
            //console.log(updateQ);
            //console.log(oracleparams);
            coreDB.executeSql(oracleconnection,updateQ,oracleparams,oraclefmt,function(error,result){
                if(error){
                    coreDB.doRollBack(oracleconnection);
                    coreDB.doRelease(oracleconnection);
                    outJson["status"]="FAIL";
                    outJson["message"]="Error In update lcl_xrt method!"+error.message;
                    callback(null,outJson);
                }else{
                    //console.log(result)
                    var rowCount = result.rowsAffected;
                   
                        coreDB.doCommit(oracleconnection);
                        var insertQ="insert into lcl_xrt(cur,xrt,fr_dte)values(:currency,:exhRte,sysdate)";
            
                        oracleparams = {};
                        oracleparams={currency,exhRte};
                        // console.log(insertQ);
                        // console.log(oracleparams);
                        coreDB.executeSql(oracleconnection,insertQ,oracleparams,oraclefmt,function(error,result){
                            if(error){
                                coreDB.doRollBack(oracleconnection);
                                coreDB.doRelease(oracleconnection);
                                outJson["status"]="FAIL";
                                outJson["message"]="Error In insert lcl_xrt Method!"+error.message;
                                callback(null,outJson);
                            }else{
                                coreDB.doCommit(oracleconnection);
                                coreDB.doRelease(oracleconnection);
                                var rowCount = result.rowsAffected;
                                if(rowCount!=0){     
                                    //console.log("DONE");
                                    outJson["status"] = "SUCCESS";
                                    outJson["message"] = "SUCCESS";
                                    callback(null, outJson);
                                }else{
                                    outJson["status"]="FAIL";
                                    outJson["message"]="Xrt Insertion Failed!";
                                    callback(null,outJson);
                                } 
                            }
                        })
                     
                }
            });
        }
    })
}

function execSendXrtMail(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        xrtMailSend(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function xrtMailSend(connection,paramJson,callback){
    var formatNme = paramJson.formatNme || '';
    var coIdn = paramJson.coIdn || '';
    var status = paramJson.status || '';
    var message = paramJson.message || '';
    let prefix = paramJson.prefix || '';
    var outJson = {};
    var logDtl = {};
    var cachedUrl = require('qaq-core-util').cachedUrl;
    if(formatNme != '' && coIdn !=''){
        var params = {
            "db":connection,
            "format":formatNme,
            "coIdn":coIdn
        }
        coreUtil.mailFormat(params).then(data =>{
            data = data || {};
            var isData=Object.keys(data) || [];
            var isDatalen=isData.length;
            if(isDatalen > 0){
                if(data["status"] == 'SUCCESS'){
                    //console.log(data);status
                    var subject = data["subject"];
                    var d=new Date();
                    d.setHours(d.getHours() + 5);
                    d.setMinutes(d.getMinutes() + 30);
                    let dte = dateFormat(d,'dd-mm-yyyy hh:MM:ss')
                    
                    var body = data["body"];
                    body = body.replace("~tm",dte); 
                    body = body.replace("~status",status);  
                    body = body.replace("~msg",message);
                    
                    var recipientlist = data["recipientlist"];
                    var to = recipientlist["to"];
                    var cc= recipientlist["cc"];
                    var bcc = recipientlist["bcc"];
                  //  console.log("to "+to);
                  //  console.log("subject "+subject);
                   // console.log("body "+body);

                    coreUtil.getCache(prefix+"dbms_"+coIdn,cachedUrl).then(dbmsDtldata =>{
                        if(dbmsDtldata == null){
                               outJson["status"]="FAIL";
                               outJson["message"]="Fail to get DBMS Attribute";
                               callback(null,outJson);
                        }else{ 
                            dbmsDtldata = JSON.parse(dbmsDtldata);
                            var smtpuser = dbmsDtldata["smtpuser"];
                            var smtppassword = dbmsDtldata["smtppassword"];
                            var smtphost = dbmsDtldata["smtphost"];
                            var smtpport = dbmsDtldata["smtpport"];
                            var senderId = dbmsDtldata["senderid"];
                    
                            var mailOptions = {
                                smtphost:smtphost,
                                smtpuser:smtpuser,
                                smtppassword:smtppassword,
                                smtpport:smtpport,
                                secure:true,
                                from: senderId, // sender address
                                cc:cc,
                                bcc:bcc,
                                to: to, // list of receivers
                                subject: subject, // Subject line
                                html: body // html body
                                };
                            //  console.log(mailOptions);
                            coreUtil.sendMail(mailOptions);
                            
                            outJson["result"]='';
                            outJson["status"]="SUCCESS";
                            outJson["message"]="Mail Sent Successfully!";
                            callback(null,outJson);
                        }
                    })
                }else{
                    callback(null,data);
                }
            } else{
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Please Verify Format or Client Idn Parameter!";
                callback(null,outJson);
            }    
         });   
    }else if(formatNme == ''){
         outJson["result"]='';
         outJson["status"]="FAIL";
         outJson["message"]="Please Verify Format Name Parameter";
         callback(null,outJson);
    }else if(coIdn == ''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Company Idn Parameter";
        callback(null,outJson);
   }
}

function execGetXrtNew(methodParam){
    return new Promise(function(resolve,reject) {
        getXrtNew(methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function getXrtNew(methodParam,callback){
    let outJson = {};

    var url = "https://www.moneycontrol.com/mccode/currencies/";

    https.get(url, (resp) => {
        let data = '';

        // A chunk of data has been recieved.
        resp.on('data', (chunk) => {
            data += chunk;
        });
        
        // The whole response has been received. Print out the result.
        resp.on('end', () => {
            let htmlData = data.toString();
            //console.log("htmlData",htmlData)
            let str = 'FR gd14"><strong>';
            let fullPageArr = htmlData.split(str);
            let valArr = fullPageArr[1] || '';
            //console.log("valArr",valArr);
            let val = valArr.split("uparrow_rd_rad");
            let valFirst = val[0] || '';
            //console.log("valFirst",valFirst)
            let strong = valFirst.split("<span class");
            let strongFirst = strong[0] || '';
            //strongFirst = replaceall("'><strong>","",strongFirst);
            console.log("MoneyControlXRT: ",strongFirst);
            outJson["data"] = strongFirst;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson);
        });

    }).on("error", (err) => {
        console.log("Error: " + err.message);
        outJson["status"]="FAIL";
        outJson["message"]="Error In get xrt from money control!"+err.message;
        callback(null,outJson);
    });
}

exports.updateMFGData = function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;  
    var outJson={};

    let methodParam = {};
    methodParam["coIdn"] = coIdn;
    methodParam["log_idn"] = log_idn;
    methodParam["poolName"] = poolName;
    let pktResult = execUpdateMFGPlanDetails(methodParam);
    outJson["status"]="SUCCESS";
    outJson["message"]="MFG data updated successfully";
    callback(null,outJson);
}

function execUpdateMFGPlanDetails(methodParam){
    return new Promise(function(resolve,reject) {
        updateMFGPlanDetails(methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateMFGPlanDetails(paramJson,callback){
    let coIdn = paramJson.coIdn;
    let log_idn = paramJson.log_idn;
    let poolName = paramJson.poolName;  
    let dtl = {};
    let outJson = {};

    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || 'TPOOL';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,async function(error,tpoolconn){
            if(error){
                console.log(error);
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                let methodParam = {};
                methodParam["dtl"] = dtl;
                methodParam["coIdn"] = coIdn;
                methodParam["log_idn"] = log_idn;
                let updateMfgDataResult = await execUpdateMFGPlanPktFromStockM(methodParam,tpoolconn);

                //methodParam = {};
                //methodParam["dtl"] = dtl;
                //methodParam["coIdn"] = coIdn;
                //methodParam["log_idn"] = log_idn;
                //updateMfgDataResult = await execUpdateMFGPlanPktStatus(methodParam,tpoolconn);
                
                let methodParams = {};
                methodParams["logDetails"] = dtl;
                methodParams["log_idn"] = log_idn;
                let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);

                coreDB.doTransRelease(tpoolconn);
                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                callback(null,outJson);
            }
        })
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }    
}

function execUpdateMFGPlanPktStatus(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        updateMFGPlanPktStatus(methodParam,tpoolconn, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateMFGPlanPktStatus(methodParam,tpoolconn,callback){
    var coIdn = methodParam.coIdn;
    let log_idn = methodParam.log_idn;
    let dtl = methodParam.dtl || {};
    let params=[];
    let fmt = {};
    let outJson = {};
    //console.log("attrIdList",attrIdList)
    
    let sql="update mfg_plan_pkt_t t set stt = 0 \n"+
        "from ( \n"+
        "select count(*) \n"+
        "cnt, max(attr_id) attr_id, lot_no  \n"+
        "from mfg_plan_pkt_t where stt = 1 group by lot_no having count(*) > 1 ) d \n"+
        " where t.lot_no = d.lot_no and t.attr_id <> d.attr_id \n"+
        "--and t.lot_no = 'CCKVOO-42.1' ";

    params=[];
    //console.log(sql);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn,sql,params,fmt,async function(error,result){
        if(error){
            console.log(error)
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"]="FAIL";
            outJson["message"]="Error In update mfg_plan_pkt_t Method!"+error.message;
            callback(null,outJson);
        }else{
            var rowCount = result.rowCount;
            dtl["mfgStatusUpdateCount"] = rowCount;
            let methodParams = {};
            methodParams["logDetails"] = dtl;
            methodParams["log_idn"] = log_idn;
            let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);
            if(rowCount>0){
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="mfg_plan_pkt_t updation failed!";
                callback(null,outJson);
            } 
        }
    })        
}

exports.sendSaleDataMails = function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;  
    var outJson={};

    let days = req.body.days || 0;
    let reportType = "Daily";
    let d=new Date();
    let todate = dateFormat(d,'dd-mm-yyyy');
    let date = new Date();
    date.setDate(date.getDate() - days);
    let weekdate = dateFormat(date,'dd-mm-yyyy');  
    let fromDte = weekdate;
    let toDte = todate;
    let mailDate = '';
    if(days == 0){ 
        mailDate = todate;
    } 
    if(days == 5){
        mailDate = fromDte+" To "+toDte;
        reportType = "Weekly";
    } 
    

    let methodParam = {};
    methodParam["coIdn"] = coIdn;
    methodParam["log_idn"] = log_idn;
    methodParam["poolName"] = poolName;
    methodParam["fromDte"] = fromDte;
    methodParam["toDte"] = toDte;
    methodParam["mailDate"] = mailDate;
    methodParam["reportType"] = reportType;
    let mailResult = execSendSaleDetailsMail(methodParam);

    outJson["status"]="SUCCESS";
    outJson["message"]="Sending mail is in process";
    callback(null,outJson);
}

function execSendSaleDetailsMail(methodParam){
    return new Promise(function(resolve,reject) {
        sendSaleDetailsMail(methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function sendSaleDetailsMail(paramJson,callback){
    let coIdn = paramJson.coIdn;
    let log_idn = paramJson.log_idn;
    let poolName = paramJson.poolName;  
    let fromDte = paramJson.fromDte;
    let toDte = paramJson.toDte;
    let mailDate = paramJson.mailDate;
    let reportType = paramJson.reportType;
    let dtl = {};
    let outJson = {};

    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || 'TPOOL';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,async function(error,tpoolconn){
            if(error){
                console.log(error);
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                let soldSummary = "";
                let dlvSummary = "";
                let salePrsSummary = "";
                let mixSaleSummary = "";
                let ttlSaleCustCnt = "";

                let methodParam = {};
                methodParam["fromDte"] = fromDte;
                methodParam["coIdn"] = coIdn;
                methodParam["toDte"] = toDte;
                let soldResult = await execGetSoldDtl(methodParam,tpoolconn);
                if(soldResult.status == 'SUCCESS')
                    soldSummary = soldResult.result || '';

                methodParam = {};
                methodParam["fromDte"] = fromDte;
                methodParam["coIdn"] = coIdn;
                methodParam["toDte"] = toDte;
                let dlvResult = await execGetDeliveryDtl(methodParam,tpoolconn);
                if(dlvResult.status == 'SUCCESS')
                    dlvSummary = dlvResult.result || '';

                methodParam = {};
                methodParam["fromDte"] = fromDte;
                methodParam["coIdn"] = coIdn;
                methodParam["toDte"] = toDte;
                let salePersonResult = await execGetSalePersonDtl(methodParam,tpoolconn);
                if(salePersonResult.status == 'SUCCESS')
                    salePrsSummary = salePersonResult.result || '';

                //methodParam = {};
                //methodParam["fromDte"] = fromDte;
                //methodParam["coIdn"] = coIdn;
                //methodParam["toDte"] = toDte;
                //let mixSaleResult = await execGetMixSaleDtl(methodParam,tpoolconn);
                //if(mixSaleResult.status == 'SUCCESS')
                //    mixSaleSummary = mixSaleResult.result || '';

                methodParam = {};
                methodParam["fromDte"] = fromDte;
                methodParam["coIdn"] = coIdn;
                methodParam["toDte"] = toDte;
                let saleCustCntResult = await execGetCustomerSaleCntDtl(methodParam,tpoolconn);
                if(saleCustCntResult.status == 'SUCCESS')
                    ttlSaleCustCnt = saleCustCntResult.result || '';

                let msg = "";
                methodParam = {};
                methodParam["mailDate"] = mailDate;
                methodParam["coIdn"] = coIdn;
                methodParam["soldSummary"] = soldSummary;
                methodParam["dlvSummary"] = dlvSummary;
                methodParam["salePrsSummary"] = salePrsSummary;
                methodParam["mixSaleSummary"] = mixSaleSummary;
                methodParam["ttlSaleCustCnt"] = ttlSaleCustCnt;
                methodParam["formatNme"] = "saleSummary";
                methodParam["reportType"] = reportType;
                let saleMailResult = await execMailSendSaleSummary(methodParam,tpoolconn);
                if(saleMailResult.status == 'SUCCESS')   
                    msg = "Mail Sent Successfully";
                else
                    msg = "Mail Failed";

                dtl["saleSummaryMail"] = msg;
                let methodParams = {};
                methodParams["logDetails"] = dtl;
                methodParams["log_idn"] = log_idn;
                let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);

                coreDB.doTransRelease(tpoolconn);
                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                callback(null,outJson);
            }
        })
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }    
}

function execGetSoldDtl(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getSoldDtl(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getSoldDtl( tpoolconn, redirectParam, callback) {
    let coIdn = redirectParam.coIdn;
    let fromDte = redirectParam.fromDte || '';
    let toDte = redirectParam.toDte || '';
    let msg = "";
    let outJson = {};

    var sql = "with ana as ( \n"+
        "select sm.stock_idn \n"+
        ", tds.qty \n"+
        ", trunc((COALESCE(tds.sal_rte,tds.quot)/COALESCE(tds.sal_exh_rte, tds.quot_exh_rte)),2) salRte \n"+
        ", tds.weight crtwt \n"+
        ", case when cast(attr ->> 'sh' as int) = 10 then 'Round' else 'Fancy' end as shape \n"+
        "from transaction_sales ts,transaction_d_sales tds,stock_m sm,stock_process sp \n"+
        "where ts.transaction_sales_idn = tds.transaction_sales_idn and tds.stock_idn = sm.stock_idn \n"+
        "and ts.process_idn = sp.process_idn and sm.co_idn = sp.co_idn and ts.co_idn=sp.co_idn \n"+
        "and sm.co_idn=$1 and sm.stock_type='NR' and sp.nme in ('lsale','sale','branch_sale') \n"+
        " and tds.status in ('CF','IS') and coalesce(sm.attr ->> 'special', '10') != '60' \n"+ // 22Jan 2021 told by purav sir to remove stone of attr-special = private value
        " and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy') ) \n"+
        "select shape, sum(qty) qty, trunc(sum(crtwt),2) crtwt, trunc(sum(salRte*crtwt)/1000,2) vlu   from ana \n"+
        "group by shape order by shape desc ";

    let params = [];
    params.push(coIdn);

    //console.log(sql)
    //console.log(params)
    coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
        if (error) {
            console.log(error);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In getSoldDtl Method!" + error.message;
            callback(null, outJson);
        } else {
            let len = result.rows.length;
            if (len > 0) {              
                msg+= "<table width='50%' class='divTr' cellspacing='0' cellpadding='0' border='1' style='border-collapse: collapse; border:1px solid black;'><tr align='center' style='background:#d5cdf4;'>"  
                msg+="<th>Shape</th><th>Qty</th><th>Crtwt</th><th>Value</th></tr>";
                let ttlQty = 0;
                let ttlCts = 0;
                let ttlVlu = 0;
                for (let i = 0; i < len; i++) {
                    var resultRow = result.rows[i];
                    let shape = resultRow["shape"] || '';
                    let qty = resultRow["qty"] || '0';
                    let crtwt = resultRow["crtwt"] || '0';
                    let vlu = resultRow["vlu"] || '0';
                    ttlQty += parseInt(qty);
                    ttlCts += parseFloat(crtwt);
                    ttlVlu += parseFloat(vlu);
                    msg+= "<tr><td>"+shape+"</td><td align='right'>"+qty+"</td><td align='right'>"+crtwt+"</td><td align='right'>"+vlu+"</td></tr>";
                    
                }

                var sql = "select sum(tds.qty) qty \n"+
                    ", trunc(sum((tds.weight + CAST(COALESCE(NULLIF(tds.issval ->> 'wt_diff', ''), '0') AS numeric)) * COALESCE(tds.sal_rte,tds.quot))/1000,2) vlu \n"+
                    ", sum(tds.weight + CAST(COALESCE(NULLIF(tds.issval ->> 'wt_diff', ''), '0') AS numeric)) crtwt  \n"+
                    "from transaction_sales ts,transaction_d_sales tds,stock_m sm,stock_process sp \n"+
                    "where ts.transaction_sales_idn = tds.transaction_sales_idn and tds.stock_idn = sm.stock_idn \n"+
                    "and ts.process_idn = sp.process_idn and sm.co_idn = sp.co_idn and ts.co_idn=sp.co_idn \n"+
                    "and sm.co_idn=$1 and sm.stock_type in ('MIX','SMX') and coalesce(sm.attr ->> 'special', '10') != '60' and tds.status in ('CF','IS') and sp.nme in ('mix_sale','mix_branch_sale','mix_lsale') \n"+
                    "and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy') ";

                let params = [];
                params.push(coIdn);

                //console.log(sql)
                //console.log(params)
                coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
                    if (error) {
                        console.log(error);
                        outJson["status"] = "FAIL";
                        outJson["message"] = "Error In getMixSaleDtl Method!" + error.message;
                        callback(null, outJson);
                    } else {
                        len = result.rows.length;
                        //console.log("len",len)
                        if (len > 0) { 
                            let resultRows = result.rows[0] || {};
                            let qtys = resultRows["qty"] || '';
                            let crtwts = resultRows["crtwt"] || '';
                            let vlus = resultRows["vlu"] || '';
                            if(qtys != ''){
                                ttlQty += parseInt(qtys);
                                ttlCts += parseFloat(crtwts);
                                ttlVlu += parseFloat(vlus);
                                msg+= "<tr><td>Parcel</td><td align='right'>"+qtys+"</td><td align='right'>"+crtwts+"</td><td align='right'>"+vlus+"</td></tr>";
                            }
                        } 
                        ttlCts = coreUtil.floorFigure(ttlCts,2);
                        ttlVlu = coreUtil.floorFigure(ttlVlu,2);
                        msg+= "<tr><td><b>Total</b></td><td align='right'><b>"+ttlQty+"</b></td><td align='right'><b>"+ttlCts+"</b></td><td align='right'><b>"+ttlVlu+"</b></td></tr>";
                        msg+= "</table>";

                        outJson["result"] = msg;
                        outJson["status"] = "SUCCESS";
                        outJson["message"] = "SUCCESS";
                        callback(null, outJson); 
                    }
                })
            } else {
                outJson["result"] = msg;
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson); 
            }
        }
    }) 
}

function execGetDeliveryDtl(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getDeliveryDtl(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getDeliveryDtl( tpoolconn, redirectParam, callback) {
    let coIdn = redirectParam.coIdn;
    let fromDte = redirectParam.fromDte || '';
    let toDte = redirectParam.toDte || '';
    let msg = "";
    let outJson = {};

    var sql = "with ana as ( \n"+
        "select sm.stock_idn \n"+
        ", tds.qty \n"+
        ", trunc((COALESCE(tds.sal_rte,tds.quot)/COALESCE(tds.sal_exh_rte, tds.quot_exh_rte)),2) salRte \n"+
        ", tds.weight crtwt \n"+
        ", case when cast(attr ->> 'sh' as int) = 10 then 'Round' else 'Fancy' end as shape \n"+
        "from transaction_sales ts,transaction_d_sales tds,stock_m sm,stock_process sp \n"+
        "where ts.transaction_sales_idn = tds.transaction_sales_idn and tds.stock_idn = sm.stock_idn \n"+
        "and ts.process_idn = sp.process_idn and sm.co_idn = sp.co_idn and ts.co_idn=sp.co_idn \n"+
        "and sm.co_idn=$1 and sm.stock_type='NR' and sp.nme in ('delivery','branch_delivery')  \n"+
        " and tds.status in ('CF','IS') and coalesce(sm.attr ->> 'special', '10') != '60' \n"+
        " and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy') ) \n"+
        "select shape, sum(qty) qty, trunc(sum(crtwt),2) crtwt,  trunc(sum(salRte*crtwt)/1000,2) vlu   from ana \n"+
        "group by shape order by shape desc ";

    let params = [];
    params.push(coIdn);

    //console.log(sql)
    //console.log(params)
    coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
        if (error) {
            console.log(error);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In getDeliveryDtl Method!" + error.message;
            callback(null, outJson);
        } else {
            let len = result.rows.length;
            if (len > 0) { 
                msg+= "<table width='50%' class='divTr' cellspacing='0' cellpadding='0' border='1' style='border-collapse: collapse; border:1px solid black;'><tr align='center' style='background:#d5cdf4;'>"  
                msg+="<th>Shape</th><th>Qty</th><th>Crtwt</th><th>Value</th></tr>";
                let ttlQty = 0;
                let ttlCts = 0;
                let ttlVlu = 0;
                for (let i = 0; i < len; i++) {
                    var resultRow = result.rows[i];
                    let shape = resultRow["shape"] || '';
                    let qty = resultRow["qty"] || '0';
                    let crtwt = resultRow["crtwt"] || '0';
                    let vlu = resultRow["vlu"] || '0';
                    ttlQty += parseInt(qty);
                    ttlCts += parseFloat(crtwt);
                    ttlVlu += parseFloat(vlu);
                    msg+= "<tr><td>"+shape+"</td><td align='right'>"+qty+"</td><td align='right'>"+crtwt+"</td><td align='right'>"+vlu+"</td></tr>";
                    
                }
                var sql = "select sum(tds.qty) qty \n"+
                ", trunc(sum(trunc(tds.weight + CAST(COALESCE(NULLIF(tds.issval ->> 'wt_diff', ''), '0') AS numeric),3) * (COALESCE(tds.sal_rte,tds.quot) /COALESCE(tds.sal_exh_rte, tds.quot_exh_rte)))/1000,2) vlu \n"+
                ", trunc(sum(tds.weight + CAST(COALESCE(NULLIF(tds.issval ->> 'wt_diff', ''), '0') AS numeric)),3) crtwt  \n"+
                "from transaction_sales ts,transaction_d_sales tds,stock_m sm,stock_process sp \n"+
                "where ts.transaction_sales_idn = tds.transaction_sales_idn and tds.stock_idn = sm.stock_idn \n"+
                "and ts.process_idn = sp.process_idn and sm.co_idn = sp.co_idn and ts.co_idn=sp.co_idn \n"+
                "and sm.co_idn=$1 and sm.stock_type in ('MIX','SMX') and coalesce(sm.attr ->> 'special', '10') != '60' and tds.status in ('CF','IS') and sp.nme in ('mix_delivery','mix_branch_delivery') \n"+
                "and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy') ";

                let params = [];
                params.push(coIdn);

                //console.log(sql)
                //console.log(params)
                coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
                    if (error) {
                        console.log(error);
                        outJson["status"] = "FAIL";
                        outJson["message"] = "Error In getMixDeliveryDtl Method!" + error.message;
                        callback(null, outJson);
                    } else {
                        len = result.rows.length;
                        //console.log("len",len)
                        if (len > 0) { 
                            let resultRows = result.rows[0] || {};
                            let qtys = resultRows["qty"] || '';
                            let crtwts = resultRows["crtwt"] || '';
                            let vlus = resultRows["vlu"] || '';
                            if(qtys != ''){
                                ttlQty += parseInt(qtys);
                                ttlCts += parseFloat(crtwts);
                                ttlVlu += parseFloat(vlus);
                                msg+= "<tr><td>Parcel</td><td align='right'>"+qtys+"</td><td align='right'>"+crtwts+"</td><td align='right'>"+vlus+"</td></tr>";
                            }
                        } 
                        ttlCts = coreUtil.floorFigure(ttlCts,2);
                        ttlVlu = coreUtil.floorFigure(ttlVlu,2);
                        msg+= "<tr><td><b>Total</b></td><td align='right'><b>"+ttlQty+"</b></td><td align='right'><b>"+ttlCts+"</b></td><td align='right'><b>"+ttlVlu+"</b></td></tr>";
                        msg+= "</table>";

                        outJson["result"] = msg;
                        outJson["status"] = "SUCCESS";
                        outJson["message"] = "SUCCESS";
                        callback(null, outJson); 
                    }
                })
            } else {
                outJson["result"] = msg;
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson); 
            }
        }
    }) 
}

function execGetSalePersonDtl(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getSalePersonDtl(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getSalePersonDtl( tpoolconn, redirectParam, callback) {
    let coIdn = redirectParam.coIdn;
    let fromDte = redirectParam.fromDte || '';
    let toDte = redirectParam.toDte || '';
    let msg = "";
    let outJson = {};

    var sql = "select get_nme(ts.emp_idn) emp,  sum(tds.qty) qty \n"+
        ", trunc(sum((COALESCE(tds.sal_rte,tds.quot)/COALESCE(tds.sal_exh_rte, tds.quot_exh_rte)) * tds.weight)/1000,2) vlu \n"+
        ", trunc(sum(tds.weight),2) crtwt \n"+
        "from transaction_sales ts,transaction_d_sales tds,stock_m sm,stock_process sp \n"+
        "where ts.transaction_sales_idn = tds.transaction_sales_idn and tds.stock_idn = sm.stock_idn \n"+
        "and ts.process_idn = sp.process_idn and sm.co_idn = sp.co_idn and ts.co_idn=sp.co_idn \n"+
        "and sm.co_idn=$1 and sm.stock_type='NR' and sp.nme in ('lsale','sale','branch_sale') \n"+
        " and tds.status in ('CF','IS') and coalesce(sm.attr ->> 'special', '10') != '60' \n"+
        " and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy')  \n"+
        "group by ts.emp_idn order by vlu desc ";

    let params = [];
    params.push(coIdn);

    //console.log(sql)
    //console.log(params)
    coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
        if (error) {
            console.log(error);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In getSalePersonDtl Method!" + error.message;
            callback(null, outJson);
        } else {
            let len = result.rows.length;
            if (len > 0) { 
                msg+= "<table width='50%' class='divTr' cellspacing='0' cellpadding='0' border='1' style='border-collapse: collapse; border:1px solid black;'><tr align='center' style='background:#d5cdf4;'>"  
                msg+="<th>Employee</th><th>Qty</th><th>Crtwt</th><th>Value</th></tr>";
                let ttlQty = 0;
                let ttlCts = 0;
                let ttlVlu = 0;
                for (let i = 0; i < len; i++) {
                    var resultRow = result.rows[i];
                    let emp = resultRow["emp"] || '';
                    let qty = resultRow["qty"] || '0';
                    let crtwt = resultRow["crtwt"] || '0';
                    let vlu = resultRow["vlu"] || '0';
                    ttlQty += parseInt(qty);
                    ttlCts += parseFloat(crtwt);
                    ttlVlu += parseFloat(vlu);
                    msg+= "<tr><td>"+emp+"</td><td align='right'>"+qty+"</td><td align='right'>"+crtwt+"</td><td align='right'>"+vlu+"</td></tr>";
                    
                }
                ttlCts = coreUtil.floorFigure(ttlCts,2);
                //msg+= "<tr><td><b>Total</b></td><td align='right'><b>"+ttlQty+"</b></td><td align='right'><b>"+ttlCts+"</b></td><td align='right'><b>"+ttlVlu+"</b></td></tr>";
                msg+= "</table>";
            }

            outJson["result"] = msg;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson); 
        }
    }) 
}

function execGetMixSaleDtl(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getMixSaleDtl(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getMixSaleDtl( tpoolconn, redirectParam, callback) {
    let coIdn = redirectParam.coIdn;
    let fromDte = redirectParam.fromDte || '';
    let toDte = redirectParam.toDte || '';
    let msg = "";
    let outJson = {};

    var sql = "select sum(tds.qty) qty \n"+
        ", round(sum((tds.weight + CAST(COALESCE(NULLIF(tds.issval ->> 'wt_diff', ''), '0') AS numeric)) * tds.sal_rte)) vlu \n"+
        ", sum(tds.weight + CAST(COALESCE(NULLIF(tds.issval ->> 'wt_diff', ''), '0') AS numeric)) crtwt  \n"+
        "from transaction_sales ts,transaction_d_sales tds,stock_m sm,stock_process sp \n"+
        "where ts.transaction_sales_idn = tds.transaction_sales_idn and tds.stock_idn = sm.stock_idn \n"+
        "and ts.process_idn = sp.process_idn and sm.co_idn = sp.co_idn and ts.co_idn=sp.co_idn \n"+
        "and sm.co_idn=$1 and sm.stock_type in ('MIX','SMX') and coalesce(sm.attr ->> 'special', '10') != '60' and tds.status in ('CF','IS') and sp.nme in ('mix_sale','mix_branch_sale','mix_lsale') \n"+
        "and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy') ";

    let params = [];
    params.push(coIdn);

    //console.log(sql)
    //console.log(params)
    coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
        if (error) {
            console.log(error);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In getMixSaleDtl Method!" + error.message;
            callback(null, outJson);
        } else {
            let len = result.rows.length;
            if (len > 0) { 
                let resultRows = result.rows[0] || {};
                let qty = resultRows["qty"] || '';
                let crtwt = resultRows["crtwt"] || '';
                let vlu = resultRows["vlu"] || '';
                if(qty != ''){
                    msg+= "<tr><td valign='middle' align='' style='padding:3px;width:100%' colspan='2'></td></tr><tr style='background-color:#d5cdf4;font-size:1em;font-weight:bold'><td valign='middle' align='' style='color:#5a5a5a;padding:5px;width:100%' colspan='2'>Mix Sale Summary Details</td></tr><tr><td valign='middle' align='' style='padding:3px;width:100%' colspan='2'></td></tr><td colspan='2'>"
                    msg+= "<table width='50%' class='divTr' cellspacing='0' cellpadding='0' border='1' style='border-collapse: collapse; border:1px solid black;'><tr align='center' style='background:#d5cdf4;'>"  
                    msg+="<th>Qty</th><th>Crtwt</th><th>Value</th></tr>";
                    msg+= "<tr><td align='right'>"+qty+"</td><td align='right'>"+crtwt+"</td><td align='right'>"+vlu+"</td></tr>";
                    msg+= "</table>";
                    msg+= "</td></tr>";
                }
            }

            outJson["result"] = msg;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson); 
        }
    }) 
}

function execGetCustomerSaleCntDtl(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getCustomerSaleCntDtl(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getCustomerSaleCntDtl( tpoolconn, redirectParam, callback) {
    let coIdn = redirectParam.coIdn;
    let fromDte = redirectParam.fromDte || '';
    let toDte = redirectParam.toDte || '';
    let cnt = 0;
    let outJson = {};

    var sql = "with ana as ( \n"+
        "select distinct(b.nme_idn) nme_idn	from transaction_sales ts,transaction_d_sales tds,buyer_terms b,stock_process sp \n"+
        "where ts.transaction_sales_idn = tds.transaction_sales_idn and ts.buyer_terms_idn = b.buyer_terms_idn \n"+
        "and ts.process_idn = sp.process_idn  and ts.co_idn=sp.co_idn and tds.status in ('CF','IS') \n"+
        "and sp.co_idn=$1  and sp.nme in ('lsale','sale','branch_sale','mix_sale','mix_branch_sale','mix_lsale') \n"+
        "and cast(ts.trns_ts as date) between to_date('"+fromDte+"', 'dd-mm-yyyy') and  to_date('"+toDte+"', 'dd-mm-yyyy')) \n"+
        "select count(*) cnt from ana";

    let params = [];
    params.push(coIdn);

    //console.log(sql)
    //console.log(params)
    coreDB.executeTransSql(tpoolconn, sql, params, {}, function (error, result) {
        if (error) {
            console.log(error);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In getCustomerSaleCntDtl Method!" + error.message;
            callback(null, outJson);
        } else {
            let len = result.rows.length;
            if (len > 0) { 
                cnt = result.rows[0].cnt || 0;
            }

            outJson["result"] = cnt;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson); 
        }
    }) 
}

function execMailSendSaleSummary(methodParam,tpoolconn) {
    return new Promise(function (resolve, reject) {
        mailSendSaleSummary(tpoolconn,methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function mailSendSaleSummary(connection,paramJson,callback){
    let formatNme = paramJson.formatNme || '';
    let coIdn = paramJson.coIdn || '';
    let mailDate = paramJson.mailDate || '';
    let soldSummary = paramJson.soldSummary || '';
    let dlvSummary = paramJson.dlvSummary || '';
    let salePrsSummary = paramJson.salePrsSummary || '';
    let mixSaleSummary = paramJson.mixSaleSummary || '';
    let ttlSaleCustCnt = paramJson.ttlSaleCustCnt || '';
    let reportType = paramJson.reportType || '';
    
    var outJson = {};
    var cachedUrl = require('qaq-core-util').cachedUrl;
    if(formatNme != '' && coIdn !=''){
        var params = {
            "db":connection,
            "format":formatNme,
            "coIdn":coIdn
        }
        coreUtil.mailFormat(params).then(data =>{
            data = data || {};
            var isData=Object.keys(data) || [];
            var isDatalen=isData.length;
            if(isDatalen > 0){
                if(data["status"] == 'SUCCESS'){
                    //console.log(data);
                    var subject = data["subject"];
                    var d=new Date();
                    d.setHours(d.getHours() + 5);
                    d.setMinutes(d.getMinutes() + 30);
                    let dte = dateFormat(d,'dd-mm-yyyy hh:MM:ss')
                    subject = subject.replace("~dte",mailDate);
                    subject = subject.replace("~mailsubj",reportType);
                    
                    var body = data["body"];
                    body = body.replace("~mailsubj",reportType);
                    body = body.replace("~saledte",mailDate);
                    body = body.replace("~custSoldCnt",ttlSaleCustCnt);
                    body = body.replace("~soldDetails",soldSummary);
                    body = body.replace("~deliveredDetails",dlvSummary);
                    body = body.replace("~salePersonDetails",salePrsSummary);
                    body = body.replace("~mixSaleDetails",mixSaleSummary);

                    let txt = '';
                    if(dlvSummary != ''){
                        txt = '<td valign="middle" align="" style="color:#5a5a5a;padding:5px;width:100%" colspan="2">Delivered Summary Details</td>';
                    }
                    body = body.replace("~deliverySummary",txt);
                    
                      
                    var recipientlist = data["recipientlist"];
                    var to = recipientlist["to"];
                    var cc= recipientlist["cc"];
                    var bcc = recipientlist["bcc"];
                    //  console.log("to "+to);
                    //  console.log("subject "+subject);
                    // console.log("body "+body);

                    coreUtil.getCache("dbms_"+coIdn,cachedUrl).then(dbmsDtldata =>{
                        if(dbmsDtldata == null){
                               outJson["status"]="FAIL";
                               outJson["message"]="Fail to get DBMS Attribute";
                               callback(null,outJson);
                        }else{ 
                            dbmsDtldata = JSON.parse(dbmsDtldata);
                            var smtpuser = dbmsDtldata["smtpuser"];
                            var smtppassword = dbmsDtldata["smtppassword"];
                            var smtphost = dbmsDtldata["smtphost"];
                            var smtpport = dbmsDtldata["smtpport"];
                            var senderId = dbmsDtldata["senderid"];
                    
                            var mailOptions = {
                                smtphost:smtphost,
                                smtpuser:smtpuser,
                                smtppassword:smtppassword,
                                smtpport:smtpport,
                                secure:true,
                                from: senderId, // sender address
                                cc:cc,
                                bcc:bcc,
                                to: to, // list of receivers
                                subject: subject, // Subject line
                                html: body // html body
                                };
                            //  console.log(mailOptions);
                            coreUtil.sendMail(mailOptions).then(mailResult =>{
                           // console.log("mailResult",mailResult);
                            
                            
                            outJson["result"]='';
                            outJson["status"]="SUCCESS";
                            outJson["message"]="Mail Sent Successfully!";
                            callback(null,outJson);
                            })
                        }
                    })
                }else{
                    callback(null,data);
                }
            } else{
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Please Verify Format or Client Idn Parameter!";
                callback(null,outJson);
            }    
         });   
    } else if(formatNme == ''){
         outJson["result"]='';
         outJson["status"]="FAIL";
         outJson["message"]="Please Verify Format Name Parameter";
         callback(null,outJson);
    } else if(coIdn == ''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Company Idn Parameter";
        callback(null,outJson);
    }   
}

exports.deleteImage =async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var source = redirectParam.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    var cachedUrl = require('qaq-core-util').cachedUrl;
    var resultFinal={};  
    var outJson={};

    let resultView = req.body.imageAttrList || [];
    let from_days = req.body.from_days || '';
    let to_days = req.body.to_days || '';
    let resultViewlen = resultView.length;
    let startTime = new Date();
   
    if(resultViewlen > 0 && from_days != '' && to_days != ''){
        let dbmsDtldata = await coreUtil.getCache("dbms_"+coIdn,cachedUrl);
        if(dbmsDtldata == null){
                outJson["result"]=resultFinal;
                outJson["status"]="FAIL";
                outJson["message"]="Fail to get DBMS Attribute";
                callback(null,outJson);
        } 
        dbmsDtldata = JSON.parse(dbmsDtldata);
    
        let resultViewDtl = {};
        let basicPathMap = {};
        for(let k=0;k<resultViewlen;k++){
            let imageAttr = resultView[k];
            basicPathMap[imageAttr] = dbmsDtldata[imageAttr];
            resultViewDtl[imageAttr] = dbmsDtldata[imageAttr+"_path"];
        }

        let paramJson={};    
        paramJson["resultView"] = resultView;
        paramJson["coIdn"] = coIdn;
        paramJson["source"] = source;
        paramJson["resultViewDtl"] =resultViewDtl;
        paramJson["basicPathMap"] = basicPathMap;
        paramJson["from_days"] = from_days;
        paramJson["to_days"] = to_days;
        paramJson["dbmsDtldata"] = dbmsDtldata;
        paramJson["log_idn"] = log_idn;
        paramJson["poolName"] = poolName;
        let pktResult =  execGetImagePacketDetails(paramJson);
        let endTime = new Date();
        pktResult["startTime"] = startTime;
        pktResult["endTime"] = endTime;
        outJson["status"] = "SUCCESS";
        outJson["message"] = "SUCCESS";
        callback(null,outJson);   
    }  else if (resultViewlen == 0) {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify imageAttrList Can not be blank!";
        callback(null, outJson);
    } else if (from_days == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify from_days Can not be blank!";
        callback(null, outJson);
    } else if (to_days == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify to_days Can not be blank!";
        callback(null, outJson);
    }    
}

function execGetImagePacketDetails(paramJson){
    return new Promise(function(resolve,reject) {
        getImagePacketDetails(paramJson, function (error, result) {
        if(error){  
          reject(error);
         }
        resolve(result);
     })
    })
}

function getImagePacketDetails(paramJson,callback) {
    var resultView = paramJson.resultView;
    let source = paramJson.source;
    var coIdn = paramJson.coIdn;
    let resultViewDtl = paramJson.resultViewDtl;
    let basicPathMap = paramJson.basicPathMap;
    let from_days = paramJson.from_days;
    let to_days = paramJson.to_days;
    let dbmsDtldata = paramJson.dbmsDtldata;
    let log_idn = paramJson.log_idn;
    let poolName = paramJson.poolName;
    let outJson = {};
    let fmt = {};
    let params=[];
    let list = [];
    let resultViewlen = resultView.length;
    let resultViewList = [];
    let responceList = [];

   // var sql="select sm.pkt_code,sm.stock_idn,sm.status, "+
   //     "sm.attr ->>'vnm' vnm , sm.attr ->>'certno' certno ";
   //     for (let i = 0; i < resultViewlen; i++) {
   //         let attr = resultView[i];
   //         sql += ",COALESCE(sm.attr ->> '" + attr + "','') " + attr;
   //     }    
   //     sql +=" from stock_m sm,transaction_sales ts,transaction_d_sales td,stock_process sp "+
   //         "where 1 = 1  "+
   //         "and sp.process_idn = ts.process_idn and sp.sub_group = 'sale'  "+
   //         "and sp.co_idn = sm.co_idn "+
   //         "and ts.transaction_sales_idn = td.transaction_sales_idn "+
   //         "and td.stock_idn = sm.stock_idn "+
   //         "and sm.co_idn = $1 and sp.stt = 1 and td.status = 'CF' "+
   //         "and td.created_ts::date between current_date - "+to_days+" and current_date - "+from_days+" ";
   //         for (let i = 0; i < resultViewlen; i++) {
   //             let attr = resultView[i];
   //             sql += " and sm.attr ->>  '" + attr + "' <> 'N' ";
   //         }
   //         sql +=" limit 1 "; 
   var poolsList= require('qaq-core-db').poolsList;
   var pool = poolsList[poolName] || '';
   //console.log(pool)
   if(pool!=''){
       coreDB.getTransPoolConnect(pool,async function(error,tpoolconn){
           if(error){
               outJson["result"]='';
               outJson["status"]="FAIL";
               outJson["message"]="Fail To Get Conection!";
               callback(null,outJson);
           }else{
                let sql = "select sm.pkt_code,sm.stock_idn,sm.status, \n"+
                        "sm.attr ->>'vnm' vnm , sm.attr ->>'certno' certno \n";
                        for (let i = 0; i < resultViewlen; i++) {
                            let attr = resultView[i];
                            sql += ",COALESCE(sm.attr ->> '" + attr + "','N') " + attr;
                        }
                    sql += " from stock_m sm \n"+
                        "where 1 = 1 and stock_type = 'NR' \n"+ 
                        " and status in ('MKSD','BRC_MKSD') and sm.co_idn = $1 and sm.stt = 1 \n"+
                        " and length(sm.attr->> 'sal_dte') = 8 \n"+
                        "and cast(sm.attr ->> 'sal_dte' as int) between "+
                        "cast(to_char(current_date - "+to_days+",'yyyymmdd') as int) "+
                        "and cast(to_char(current_date -"+from_days+",'yyyymmdd') as int) \n";
                        //" and pkt_code = '1001199618'";
                        for (let i = 0; i < resultViewlen; i++) {
                            let attr = resultView[i];
                            if(i==0)
                                sql += " and (coalesce(sm.attr ->>  '" + attr + "', 'N') not in ('', 'N') \n";
                            else 
                                sql += " OR coalesce(sm.attr ->>  '" + attr + "', 'N') not in ('', 'N') \n";
                        }
                        sql +=" )";

                        //sql +=" limit 1 ";

                    params.push(coIdn);
                    
                    //console.log(sql);
                    //console.log(params);
                    coreDB.executeTransSql(tpoolconn,sql,params,fmt,async function(error,result){
                        if(error){
                            coreDB.doTransRelease(tpoolconn);
                            console.log(error);
                            outJson["status"]="FAIL";
                            outJson["message"]="Error In getImagePacketDetails Method!"+error.message;
                            console.log(outJson);
                            callback(null,outJson);
                        }else{
                            var len=result.rows.length;
                            //console.log(len);
                            if(len>0){
                                for(let k=0;k<len;k++){
                                    let resultRows = result.rows[k];
                                    let map = {};
                                    let imageMap = {};
                                    let responceMap = {};
                                    responceMap["pkt_code"] = resultRows.pkt_code;
                                    map["pkt_code"] = resultRows.pkt_code;
                                    map["stock_idn"] = resultRows.stock_idn;
                                    map["status"] = resultRows.status;
                                    let vnm = resultRows.vnm;
                                    //console.log("vnm",vnm);
                                    let certno = resultRows.certno;
                                    let stock_idn = resultRows.stock_idn;
                                    let resultViewMap = {};
                                    for (let j = 0; j < resultViewlen; j++) {
                                        let attr = resultView[j];
                                        //console.log("attr",attr);
                                        let attrVal = resultRows[attr] || '';
                                        if(attrVal != '' && attrVal != 'N' && attrVal != null){
                                            let imageUrlVal = resultViewDtl[attr];
                                            if(imageUrlVal.indexOf("vnm") > -1)
                                                imageUrlVal = replaceall("vnm", vnm, imageUrlVal);
                                            if(imageUrlVal.indexOf("cert_no") > -1)
                                                imageUrlVal = replaceall("cert_no", certno, imageUrlVal);

                                            imageMap[attr] = imageUrlVal;
                                            let folderName = vnm+"/"
                                            let s3url = dbmsDtldata.s3url;
                                            let fileResult = {};
                                            if(imageUrlVal.indexOf(folderName) > -1){
                                                let folderpath = basicPathMap[attr];
                                                folderpath = replaceall(s3url,"",folderpath);
                                                folderpath = replaceall("/","",folderpath);
                                                //folderName = replaceall("/","",folderName);
                                                folderpath = folderpath +"/"+folderName;
                                                //console.log("folderpath",folderpath);
                                                let methodParam = {};
                                                methodParam["folderName"] = folderpath;
                                                methodParam["dbmsDtldata"] = dbmsDtldata;
                                                fileResult = await execDeleteFolder(methodParam);
                                            } else {
                                                //console.log("before",imageUrlVal);
                                                if(imageUrlVal.indexOf(s3url+"/") > -1)
                                                    imageUrlVal = replaceall(s3url+"/", "", imageUrlVal);
                                                //console.log("after",imageUrlVal);
                                                let methodParam = {};
                                                methodParam["imageUrl"] = imageUrlVal;
                                                methodParam["dbmsDtldata"] = dbmsDtldata;
                                                fileResult = await execDeleteFile(methodParam);
                                            }
                                            
                    
                                            imageUrlVal = replaceall(basicPathMap[attr], "", imageUrlVal);                        
                                            //console.log(fileResult.status,"fileResult",fileResult.message);
                                            //console.log("imageUrlVal",imageUrlVal);
                                            responceMap[attr+"_count"] = 0;
                                            if(fileResult.status == 'SUCCESS'){
                                                resultViewMap[attr] = 'N';
                                                responceMap[attr+"_count"] = fileResult.result || 0;
                                            } else {
                                                resultViewMap[attr] = imageUrlVal;
                                            }
                                            map[attr] = attrVal; 
                                        }                      
                                    }
                                    var resultViewMapKeys=Object.keys(resultViewMap) || [];
                                    var resultViewMapKeyslen=resultViewMapKeys.length;
                                    //console.log("resultViewMapKeys",resultViewMapKeys);
                                    if(resultViewMapKeyslen > 0){
                                        let methodParamLocal = {};
                                        methodParamLocal["resultViewMap"] = resultViewMap;
                                        methodParamLocal["coIdn"] = coIdn;
                                        methodParamLocal["stock_idn"] = stock_idn;
                                        let stockResult = await execUpdateStockM(methodParamLocal,tpoolconn);

                                    }
                                    list.push(map);
                                    resultViewList.push(imageMap);
                                    responceList.push(responceMap);
                                }
                                let dtl = {};
                                dtl["deleteImageList"] = responceList;
                                let methodParams = {};
                                methodParams["logDetails"] = dtl;
                                methodParams["log_idn"] = log_idn;
                                let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);
                                //outJson["resultViewList"]=resultViewList;
                                //outJson["result"]=list;
                                coreDB.doTransRelease(tpoolconn);
                                outJson["result"]=responceList;
                                outJson["status"]="SUCCESS";
                                outJson["message"]="SUCCESS";
                                callback(null,outJson);
                            }else{
                                coreDB.doTransRelease(tpoolconn);
                                outJson["status"] = "FAIL";
                                outJson["message"] = "Sorry no result found";
                                callback(null,outJson);
                            }
                        }
                    });     
                }
            });
        }else{
            outJson["result"]='';
            outJson["status"]="FAIL";
            outJson["message"]="Fail To Get Conection!";
            callback(null,outJson);
        }
}

function execDeleteFile(methodParam) {
    return new Promise(function (resolve, reject) {
        deleteFile(methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function deleteFile(redirectParam, callback){
    let imageUrl = redirectParam.imageUrl;
    let dbmsDtldata = redirectParam.dbmsDtldata;
    let outJson = {};
    
    let params = {};
    params["accessKeyId"]=dbmsDtldata.s3key;
    params["secretAccessKey"]=dbmsDtldata.s3val;
    AWS.config.update(params);
    let s3 = new AWS.S3();

    var param = {
        Bucket: dbmsDtldata.s3bucket,
        Delete: { // required
            Objects: [ // required
              {
                Key: imageUrl// required
              }
            ]
          }
      };
	  
	   s3.deleteObjects(param,function (err,data){
        if(err){
            console.log("FAIL",err);
            outJson["status"] = "FAIL";
            outJson["message"] = "File deletion failed";
            callback(null, outJson);
        } else {
            console.log("data",data);
            let length = data.Deleted.length || 0;
            outJson["result"] = length; 
            outJson["status"] = "SUCCESS";
            outJson["message"] = "File deleted successfully";
            callback(null, outJson);
        }
    })
}

function execDeleteFolder(methodParam) {
    return new Promise(function (resolve, reject) {
        deleteFolder(methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function deleteFolder(redirectParam, callback){
    let folderName = redirectParam.folderName;
    let dbmsDtldata = redirectParam.dbmsDtldata;
    let outJson = {};
    //console.log("folderName",folderName)

    let param = {};
    param["accessKeyId"]=dbmsDtldata.s3key;
    param["secretAccessKey"]=dbmsDtldata.s3val;
    //param["region"]="ap-southeast-1";
    AWS.config.update(param);
    let s3 = new AWS.S3();

      //console.log("s3",s3);
    var params = {
        Bucket: dbmsDtldata.s3bucket,
        Prefix:folderName
    }
   
    s3.listObjects(params, function(err, data) {
        if (err) {
            console.log("Folder error",err);
            outJson["status"] = "FAIL";
            outJson["message"] = "Folder error";
            callback(null, outJson);
        }

        if (data.Contents.length == 0){
            //console.log("Folder not found");
            outJson["status"] = "SUCCESS";
            outJson["message"] = "Folder not found";
            callback(null, outJson);
        } else {
            params = {};
            params = {Bucket:  dbmsDtldata.s3bucket};
            params.Delete = {Objects:[]};
        
            data.Contents.forEach(function(content) {
                //console.log("Key",content.Key);
                params.Delete.Objects.push({Key: content.Key});
            });

            s3.deleteObjects(params, function(err, data) {
                if (err) {
                console.log("Folder deletion failed",err);
                outJson["status"] = "FAIL";
                outJson["message"] = "Folder deletion failed";
                callback(null, outJson);
                }
                //console.log(data.Deleted.length);
                if(data.Deleted.length > 0){
                    console.log("Folder deleted successfully");
                    outJson["result"] = data.Deleted.length;
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "Folder deleted successfully";
                    callback(null, outJson);
                }
                else {
                    console.log("Folder deletion Fail");
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "Folder deletion Fail";
                    callback(null, outJson);
                }
            });
        } 
    });
}

exports.checkImagesExistOfTrfDte =async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var source = redirectParam.source;
    var cachedUrl = require('qaq-core-util').cachedUrl;
    let poolName = redirectParam.poolName;
    let log_idn = redirectParam.log_idn;
    var resultFinal={};  
    var outJson={};

    let resultView = req.body.imageAttrList || [];
    let days = req.body.days || '';
    let type = req.body.type || 'trfDate';
    let biGroupList = req.body.biGroupList || [];
    let resultViewlen = resultView.length;
   
    if(resultViewlen > 0 && days != ''){
        let dbmsDtldata = await coreUtil.getCache("dbms_"+coIdn,cachedUrl);
        if(dbmsDtldata == null){
                outJson["result"]=resultFinal;
                outJson["status"]="FAIL";
                outJson["message"]="Fail to get DBMS Attribute";
                callback(null,outJson);
        } 
        dbmsDtldata = JSON.parse(dbmsDtldata);
    
        let resultViewDtl = {};
        let basicPathMap = {};
        for(let k=0;k<resultViewlen;k++){
            let imageAttr = resultView[k];
            basicPathMap[imageAttr] = dbmsDtldata[imageAttr];
            resultViewDtl[imageAttr] = dbmsDtldata[imageAttr+"_path"] || '';
        }

        let paramJson={};    
        paramJson["resultView"] = resultView;
        paramJson["coIdn"] = coIdn;
        paramJson["source"] = source;
        paramJson["resultViewDtl"] =resultViewDtl;
        paramJson["basicPathMap"] = basicPathMap;
        paramJson["days"] = days;
        paramJson["poolName"] = poolName;
        paramJson["biGroupList"] = biGroupList;
        paramJson["type"] = type;
        paramJson["log_idn"]=log_idn;
        let pktResult = execGetPacketDetailsImage(paramJson);
        outJson["result"] = resultFinal;
        outJson["status"] = "SUCCESS";
        outJson["message"] = "SUCCESS";
        callback(null, outJson);
        //if(pktResult.status == 'SUCCESS'){
        //    let packetDetails = pktResult["result"] || [];
        //    let resultViewList = pktResult["resultViewList"] || [];      
        //    callback(null,pktResult);   
        //} else {
        //    callback(null,pktResult);
        //}   
    }  else if (resultViewlen == 0) {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify imageAttrList Can not be blank!";
        callback(null, outJson);
    } else if (days == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify days Can not be blank!";
        callback(null, outJson);
    }    
}

function execGetPacketDetailsImage(paramJson){
    return new Promise(function(resolve,reject) {
        getPacketDetailsImage(paramJson, function (error, result) {
        if(error){  
          reject(error);
         }
        resolve(result);
     })
    })
}

function getPacketDetailsImage(paramJson,callback) {
    var resultView = paramJson.resultView;
    let source = paramJson.source;
    var coIdn = paramJson.coIdn;
    let resultViewDtl = paramJson.resultViewDtl || {};
    let basicPathMap = paramJson.basicPathMap;
    let days = paramJson.days || '0';
    let poolName = paramJson.poolName;
    let biGroupList = paramJson.biGroupList || [];
    let type = paramJson.type;
    let log_idn = paramJson.log_idn;
    let outJson = {};
    let fmt = {};
    let params=[];
    let list = [];
    var date = new Date();
    date.setDate(date.getDate() - parseInt(days));
    let resultViewlen = resultView.length;
    let resultViewList = [];
    let conQ = "";
    let whereQ = "";
    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || 'TPOOL';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool, function(error,tpoolconn){
            if(error){
                console.log(error);
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                for (let i = 0; i < resultViewlen; i++) {
                    let attr = resultView[i];
                    if (attr == 'crtwt')
                        conQ += ", trunc(CAST(sm.attr ->> 'crtwt' as Numeric),2)  " + attr;
                    else
                        conQ += ",COALESCE(sm.attr ->> '" + attr + "','') " + attr;

                    if(whereQ == "")
                        whereQ += " and (COALESCE(sm.attr ->> '" + attr + "','N') = 'N' ";
                    else
                        whereQ += " OR COALESCE(sm.attr ->> '" + attr + "','N') = 'N' ";
                }    

                whereQ += " )";

                var sql="";
                if(type == 'trfDate'){
                    sql = "select pkt_idn,stock_idn,sm.status,CAST(sm.attr->>'trf_dte' AS DATE) trf_dte, "+
                        "sm.attr ->>'vnm' vnm , sm.attr ->>'certno' certno "+ conQ +
                        " from stock_m sm,stock_status ss "+
                        "where sm.status = ss.status and sm.co_idn=$1 "+
                        " and sm.stt=1 and sm.stock_type='NR' "+
                        " and ss.stt=1 and ss.co_idn=$2 and "+
                        " ss.bi_group in ('cs', 'mkt','lab')   "+ 
                        "and CASE WHEN LENGTH(sm.attr ->> 'trf_dte') <> 8 then "+
                        "cast(to_char(current_date, 'YYYYMMDD') as int) "+
                        "else cast(sm.attr ->> 'trf_dte' as INT) end "+ 
                        //" and CAST(COALESCE(NULLIF(sm.attr ->> 'trf_dte', ''), '0') AS INT) \n" + 
                        " between "+dateFormat(date,'yyyymmdd')+" and "+dateFormat(new Date(),'yyyymmdd')+"\n" +  whereQ +
                        "Union "+
                        "select pkt_idn,stock_idn,sm.status,CAST(sm.attr->>'trf_dte' AS DATE) trf_dte, "+
                        "sm.attr ->>'vnm' vnm , sm.attr ->>'certno' certno "+ conQ +
                        " from stock_m sm,stock_status ss "+
                        "where sm.status = ss.status and sm.co_idn=$3 "+
                        " and sm.stt=1 and sm.stock_type='NR' "+
                        " and ss.stt=1 and ss.co_idn=$4  "+ whereQ +
                        //"and  ss.bi_group not in ('sold', 'mkt','cs', 'mix', 'na') "+
                        "and  ss.bi_group in ('pri') "+
                        "and ss.status not in ('MX_AV','PCHK','PLAN_CHK','PROINV','MKSL1') ";   

                    params.push(coIdn);
                    params.push(coIdn);
                    params.push(coIdn);
                    params.push(coIdn);
                } else {
                    sql = "select pkt_idn,stock_idn,sm.status,CAST(sm.attr->>'recpt_dt' AS DATE) trf_dte, \n"+
                        "sm.attr ->>'vnm' vnm , sm.attr ->>'certno' certno \n"+ conQ +
                        " from stock_m sm,stock_status ss \n"+
                        "where sm.status = ss.status and sm.co_idn=$1 \n"+
                        " and sm.stt=1 and sm.stock_type='NR' \n"+
                        " and ss.stt=1 and ss.co_idn=$2 and \n"+
                        " ss.bi_group in ('" + biGroupList.join("','") + "') \n"+ 
                        "and CASE WHEN LENGTH(sm.attr ->> 'recpt_dt') <> 8 then \n"+
                        "cast(to_char(current_date, 'YYYYMMDD') as int) \n"+
                        "else cast(sm.attr ->> 'recpt_dt' as INT) end \n"+ 
                        //" and CAST(COALESCE(NULLIF(sm.attr ->> 'trf_dte', ''), '0') AS INT) \n" + 
                        " between "+dateFormat(date,'yyyymmdd')+" and "+dateFormat(new Date(),'yyyymmdd')+"\n" +  whereQ ;
                        params.push(coIdn);
                        params.push(coIdn);
                }
                
                
                //console.log(sql);
                //console.log(params);
                coreDB.executeTransSql(tpoolconn,sql,params,fmt,async function(error,result){
                    if(error){
                        coreDB.doTransRelease(tpoolconn);
                        console.log(error);
                        outJson["status"]="FAIL";
                        outJson["message"]="Error In getPacketDetails Method!"+error.message;
                        console.log(outJson);
                        callback(null,outJson);
                    }else{
                        var len=result.rows.length;
                        //console.log("len",len);
                        let successCount = 0;
                        if(len>0){
                            for(let k=0;k<len;k++){
                                let resultRows = result.rows[k];
                                let map = {};
                                let imageMap = {};
                                map["pkt_idn"] = resultRows.pkt_idn;
                                map["stock_idn"] = resultRows.stock_idn;
                                map["status"] = resultRows.status;
                                map["trf_dte"] = resultRows.trf_dte;
                                let vnm = resultRows.vnm || '';
                                //console.log("vnm",vnm);
                                let certno = resultRows.certno || '';
                                let stock_idn = resultRows.stock_idn;
                                let resultViewMap = {};
                                for (let j = 0; j < resultViewlen; j++) {
                                    let attr = resultView[j];
                                    let attrVal = resultRows[attr];
                                    let imageUrlVal = resultViewDtl[attr] || '';
                                    if(imageUrlVal != ''){
                                        imageUrlVal = replaceall("vnm", vnm, imageUrlVal);
                                        imageUrlVal = replaceall("cert_no", certno, imageUrlVal);
                                        //imageUrlVal = replaceall(" ","+",imageUrlVal);
                                    }
                                    
                                    imageMap[attr] = imageUrlVal;
                                    //console.log("Before imageUrlVal",imageUrlVal);
                                    let methodParam = {};
                                    methodParam["imageUrl"] = imageUrlVal;
                                    let imageResult = await execCheckImageExist(methodParam);
                                    imageUrlVal = replaceall(basicPathMap[attr], "", imageUrlVal);                        
                                    //console.log("imageResult",imageResult);
                                    //console.log("After imageUrlVal",imageUrlVal);
                                    if(imageResult.status == 'SUCCESS'){
                                        resultViewMap[attr] = imageUrlVal;
                                        successCount++;
                                    } else {
                                        resultViewMap[attr] = 'N';
                                    }
                                    map[attr] = attrVal;                        
                                }
                                var resultViewMapKeys=Object.keys(resultViewMap) || [];
                                var resultViewMapKeyslen=resultViewMapKeys.length;
                                //console.log("resultViewMapKeys",resultViewMapKeyslen);
                                if(resultViewMapKeyslen > 0){
                                    let methodParamLocal = {};
                                    methodParamLocal["resultViewMap"] = resultViewMap;
                                    methodParamLocal["coIdn"] = coIdn;
                                    methodParamLocal["stock_idn"] = stock_idn;
                                    let stockResult = await execUpdateStockM(methodParamLocal,tpoolconn);

                                }
                                list.push(map);
                                resultViewList.push(imageMap);
                            }
                            let dtl ={};
                            dtl["totalPackets"] = len;
                            dtl["successPacketCount"] = successCount;
                            let methodParams = {};
                            methodParams["logDetails"] = dtl;
                            methodParams["log_idn"] = log_idn;
                            let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);
            
                            //console.log(resultViewList);
                            //outJson["resultViewList"]=resultViewList;
                            //outJson["result"]=list;
                            coreDB.doTransRelease(tpoolconn);
                            outJson["status"]="SUCCESS";
                            outJson["message"]="SUCCESS";
                            callback(null,outJson);
                        }else{
                            coreDB.doTransRelease(tpoolconn);
                            outJson["status"] = "FAIL";
                            outJson["message"] = "Sorry no result found";
                            callback(null,outJson);
                        }
                    }
                }); 
            }
        })
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }    
}

function execCheckImageExist(methodParam) {
    return new Promise(function (resolve, reject) {
        checkImageExist(methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function checkImageExist(redirectParam, callback){
    let imageUrl = redirectParam.imageUrl;
    let outJson = {};
    
    urlExists(imageUrl, function(err, exists) {
        if(!exists){
            //console.log("not exist");
            outJson["status"] = "FAIL";
            outJson["message"] = "Image not exist";
            callback(null, outJson);
        } else {
            //console.log("exist");
            outJson["status"] = "SUCCESS";
            outJson["message"] = "Image is exist";
            callback(null, outJson);
        }
    });
}

function execUpdateStockM(methodParam ,tpoolconn) {
    return new Promise(function (resolve, reject) {
        updateStockM(methodParam,tpoolconn, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateStockM(methodParam, tpoolconn, callback) {
    var resultViewMap = methodParam.resultViewMap;
    var coIdn = methodParam.coIdn;
    var updateStock = "";
    var stock_idn = methodParam.stock_idn;
    var outJson = {};
    var resultFinal = {};
    //console.log("stock_idn",stock_idn);
    //console.log("resultViewMap",resultViewMap);

    updateStock = "update stock_m set attr = attr || concat('" + JSON.stringify(resultViewMap) + "')::jsonb "+
        ",modified_ts=current_timestamp   where stock_idn = $1 and co_idn=$2 ";

    var params = [];
    params.push(stock_idn);
    params.push(coIdn);
    var fmt = {};
    //console.log(updateStock)
    //console.log(params)
    coreDB.executeTransSql(tpoolconn, updateStock, params, fmt, function (error, result) {
        if (error) {
            coreDB.doTransRollBack(tpoolconn);
            outJson["status"] = "FAIL";
            outJson["message"] = "Fail To Update stock_m!" + error.message;
            outJson["result"] = resultFinal;
            callback(null, outJson);
        } else {
            //coreDB.doTransCommit(tpoolconn);
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            outJson["result"] = resultFinal;
            callback(null, outJson);
        }
    });
}

exports.saveAccountData = function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    var outJson={};

    let oraclePoolName = req.body.oraclePoolName || 'KGFAPOOL';
    var fromDate = req.body.fromDate || '';
    var toDate = req.body.toDate || '';

    let methodParam = {};
    methodParam["oraclePoolName"] = oraclePoolName;
    methodParam["source"] = source;
    methodParam["coIdn"] = coIdn;
    methodParam["fromDate"] = fromDate;
    methodParam["toDate"] = toDate;
    methodParam["log_idn"] = log_idn;
    methodParam["poolName"] = poolName;
    methodParam["logUsr"] = "SYNC";
    let accResult = execSaveAccountDetails(methodParam);

    outJson["status"]="SUCCESS";
    outJson["message"]="Account data inserted successfully";
    callback(null,outJson);          
}

function execSaveAccountDetails(methodParam) {
    return new Promise(function (resolve, reject) {
        saveAccountDetails( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

async function saveAccountDetails(redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source;
    let log_idn = redirectParam.log_idn;
    let oraclePoolName = redirectParam.oraclePoolName || 'KGFAPOOL';
    var fromDate = redirectParam.fromDate || '';
    var toDate = redirectParam.toDate || '';
    let poolName = redirectParam.poolName;
    let logUsr = redirectParam.logUsr;
    var outJson={};
    let dtl = {};
   
    let methodParam = {};
    methodParam["oraclePoolName"] = oraclePoolName;
    methodParam["source"] = source;
    methodParam["coIdn"] = coIdn;
    methodParam["fromDate"] = fromDate;
    methodParam["toDate"] = toDate;
    let accResult = await execGetOracleAccDetails(methodParam);
    let accDataList = accResult["result"] || [];
    dtl["getAccountDataStatus"] =  accResult.status;
    dtl["getAccountDataMessage"] =  accResult.message;
    dtl["getAccountDataCount"] = accDataList.length;

    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || 'TPOOL';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,async function(error,tpoolconn){
            if(error){
                console.log(error);
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                let methodParams = {};
                methodParams["logDetails"] = dtl;
                methodParams["log_idn"] = log_idn;
                let logResult = await execUpdateScheduleLog(methodParams,tpoolconn);
                if(accResult.status == 'SUCCESS'){
                    accDataList = accResult["result"] || [];
                    let tileWisearrayExec = [];
                    for(let i=0;i<accDataList.length;i++){
                        let obj = accDataList[i];
                        let paramJson={};
                        paramJson["inv_date"] = obj.inv_date;  
                        paramJson["pkt_code"] = obj.mstk_idn; 
                        paramJson["inv_no"] = obj.inv_no; 
                        paramJson["fe_rate"] = obj.fe_rate;
                        paramJson["rs_rate"] = obj.rs_rate;
                        paramJson["logUsr"] =logUsr;
                        tileWisearrayExec.push(function (callback) { updateAccountData(paramJson, tpoolconn,callback); });
                    }
                    async.parallel(tileWisearrayExec,async function (err, result) {
                        if (err) {
                            console.log(err);
                            coreDB.doTransRelease(tpoolconn);
                            outJson["status"]="FAIL";
                            outJson["message"]="Error In updateEntries Method!";
                            callback(null,outJson);
                        } else {
                            coreDB.doTransRelease(tpoolconn);
                            outJson["status"]="SUCCESS";
                            outJson["message"]="SUCCESS";
                            callback(null,outJson);
                        }
                    })   
                }else{
                    coreDB.doTransRelease(tpoolconn);
                    callback(null,accResult);
                }  
            }
        })
    }else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }            
}

function execGetOracleAccDetails(methodParam) {
    return new Promise(function (resolve, reject) {
        getOracleAccDetails( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getOracleAccDetails(paramJson,callback) {
    var coIdn = paramJson.coIdn;
    let oraclePoolName = paramJson.oraclePoolName;
    let source = paramJson.source;
    let fromDate = paramJson.fromDate;
    let toDate = paramJson.toDate;
    let outJson = {};
    var accDataList = [];
    oraclePoolName = oraclePoolName.trim();
    coreDB.getPoolConnect(oraclePoolName,function(error,oracleconnection){
        if(error){
                console.log(error);
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Oracle Connection!";
                callback(null,outJson);   
        }else{

            let fmt = {outFormat: oracledb.OBJECT};
            let params = {};
            
            var sql="select to_char(inv_date, 'yyyymmdd') inv_date, mstk_idn, \n"+
                "inv_no, decode(currency_prefix, 'RS', trunc(rate/bank_rate,2), rate) fe_rate \n"+
                ", decode(currency_prefix, 'RS', rate, trunc(rate*bank_rate,2)) rs_rate \n"+
                "from ie_sal_item \n"+
                "where inv_date between to_date('"+fromDate+"','dd/mm/yyyy') and to_date('"+toDate+"','dd/mm/yyyy') \n"+
                //"and mstk_idn in (5014257,5056184,5021754,5044887) "+
                "order by 1 desc ";

            //console.log(sql)
            //console.log(params)
            coreDB.executeSql(oracleconnection,sql,params,fmt,function(error,result){
                if(error){
                    console.log(error);
                    coreDB.doRelease(oracleconnection);
                    outJson["status"]="FAIL";
                    outJson["message"]="Error In getOracleAccDetails Method!"+error.message;
                    callback(null,outJson);
                }else{
                    var len=result.rows.length;
                    //console.log("len",len);
                    if(len>0){
                        for(let i =0 ;i<len;i++){                                   
                            let data = result.rows[i];
                            let k = {};
                            k["inv_date"]=data["INV_DATE"];
                            k["mstk_idn"]=data["MSTK_IDN"]; 
                            k["inv_no"]= data["INV_NO"];
                            k["fe_rate"]=data["FE_RATE"];
                            k["rs_rate"]=data["RS_RATE"];  
                            
                            accDataList.push(k);                                      
                        }

                        console.log("accDataList",accDataList.length);
                        coreDB.doRelease(oracleconnection);
                        outJson["status"]="SUCCESS";
                        outJson["message"]="SUCCESS";
                        outJson["result"]=accDataList;
                        callback(null,outJson);
                    }else{
                        coreDB.doRelease(oracleconnection);
                        outJson["status"]="FAIL";
                        outJson["message"]="Sorry result not found";
                        outJson["result"]=accDataList;
                        callback(null,outJson);
                    }
                }
            })
        }
    })

}

function updateAccountData(methodParam, tpoolconn, callback) {
    let inv_date = methodParam.inv_date;
    let pkt_code = methodParam.pkt_code;
    let inv_no = methodParam.inv_no;
    let fe_rate = methodParam.fe_rate;
    let rs_rate = methodParam.rs_rate;
    let logUsr = methodParam.logUsr;
    let attrDtl = {};
    attrDtl["acc_inv_date"] = inv_date;
    attrDtl["acc_inv_no"] = inv_no;
    attrDtl["acc_per_rs"] = rs_rate;
    attrDtl["acc_per_fe"] = fe_rate;

    let fmt = {};
    let params = [];
    let outJson = {};
    var sql = "update stock_m  set attr = attr || '" + JSON.stringify(attrDtl) + "' "+
        ", modified_ts = current_timestamp, modified_by = $1 "+
        " where pkt_code = $2 ";

    params.push(logUsr);
    params.push(pkt_code);
    //console.log(sql);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, sql, params, fmt, function (error, result) {
        if (error) {
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In update stock_m Method!";
            callback(null, outJson);
        } else {
            var len = result.rowCount;
            if (len > 0) {
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            } else {
                outJson["status"] = "FAIL";
                outJson["message"] = "Stock Master not updated";
                callback(null, outJson);
            }  
        }
    });
}

exports.fullStockSync = async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    let prefix = redirectParam.prefix || '';
    var outJson={};

    let portalList = req.body.portalList || [];
    let process = req.body.process || '';
    let days = req.body.days || '';
    let minutes = req.body.minutes || '';
    let scheduleYN = req.body.scheduleYN || 'Y';
    let processArry = [];
    if(process == 'update'){
        processArry.push('delete');
        processArry.push('status');
    } else {
        processArry.push(process);
    }

    for(let i=0;i<processArry.length;i++){
        let processNme = processArry[i] || '';
        //console.log("processNme",processNme);
        let methodParam = {};
        methodParam["coIdn"] = coIdn;
        methodParam["process"] = processNme;
        methodParam["scheduleYN"] = scheduleYN;
        methodParam["poolName"] = poolName;
        methodParam["days"] = days;
        methodParam["minutes"] = minutes;
        methodParam["portalList"] = portalList;
        methodParam["source"] = source;
        methodParam["prefix"] = prefix;
        let syncResult =await execGetSyncQueryStart(methodParam,tpoolconn);
    } 
    outJson["status"] = "SUCCESS";
    outJson["message"] = "SUCCESS";
    callback(null, outJson);   
}

function execGetSyncQueryStart(methodParam,tpoolconn) {
    return new Promise(function (resolve, reject) {
        getSyncQueryStart( methodParam,tpoolconn, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function getSyncQueryStart(redirectParam,tpoolconn,callback) {
    let coIdn = redirectParam.coIdn;
    let process = redirectParam.process || '';
    let scheduleYN = redirectParam.scheduleYN || "N";
    let poolName = redirectParam.poolName;
    let portalList = redirectParam.portalList || [];
    let days = redirectParam.days || '';
    let minutes =  redirectParam.minutes || '';
    let source = redirectParam.source;
    let prefix = redirectParam.prefix || '';
    var outJson={};

    let fmt = {};
    let params = [];
    var sql = "select p.nme, unnest(p.co_allow) cos, o.refresh_min,o.file_idn,o.updates_min,p.service_url \n"+
        "from portal_sync p, file_options o  where \n"+
        "  o.portal_idn = p.portal_idn and o.stt=1  and o.co_idn = any(p.co_allow) \n";
        let cnt = 0;

        if(process != ''){
            cnt++;
            sql +=" and $"+cnt+" = ANY(p.methods) \n";
            params.push(process);
        }
        if(portalList.length > 0){
            sql +=" and p.nme in ('"+portalList.join("','")+"') \n";
        }
        if(process == 'refresh' && scheduleYN == 'Y'){
           sql +=  " and COALESCE(next_refresh_ts,CURRENT_TIMESTAMP) <= current_timestamp + interval '3 minute'  ";
        }
        sql +=" order by nme ";
        
    //console.log(sql);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, sql, params, fmt, function (error, result) {
        if (error) {
            console.log(error);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In PortalSync Method!";
            callback(null, outJson);
        } else {
            var len = result.rows.length;
            //console.log(len);
            if (len > 0) {
                let methodParam = {}; 
                methodParam["syncData"] = result.rows;
                methodParam["process"] = process;
                methodParam["scheduleYN"] = scheduleYN;
                methodParam["poolName"] = poolName;
                methodParam["days"] = days;
                methodParam["minutes"] = minutes;
                methodParam["source"] = source;
                methodParam["prefix"] = prefix;
                let syncResult = execGetSyncStart(methodParam);
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            } else {
                outJson["status"] = "FAIL";
                outJson["message"] = "Sorry no result found";
                callback(null, outJson);
            }  
        }
    }); 
}

function execGetSyncStart(methodParam) {
    return new Promise(function (resolve, reject) {
        getSyncStart( methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getSyncStart(redirectParam,callback) {
    let syncData = redirectParam.syncData || [];
    let process = redirectParam.process || '';
    let scheduleYN = redirectParam.scheduleYN || "N";
    let poolName = redirectParam.poolName;
    let days = redirectParam.days || '';
    let minutes =  redirectParam.minutes || '';
    let source = redirectParam.source;
    let prefix = redirectParam.prefix || '';
    var outJson={};

    for(let i=0;i<syncData.length;i++){
        let data = syncData[i] || {};
        let methodParam = {};
        let portal = data.nme;
        methodParam["coIdn"] = data.cos;
        methodParam["fileIdn"] = data.file_idn;
        methodParam["refresh_min"] = data.refresh_min;
        methodParam["updates_min"] = data.updates_min;
        methodParam["portal"] = portal;
        methodParam["process"] = process;
        methodParam["scheduleYN"] = scheduleYN;
        methodParam["poolName"] = poolName;
        methodParam["days"] = days;
        methodParam["minutes"] = minutes;
        methodParam["service_url"] = data.service_url;
        methodParam["source"] = source;
        methodParam["prefix"] = prefix;
        //console.log("methodParam",methodParam);
        let syncResult =await execGetSyncDtl(methodParam);
    } 
    outJson["status"] = "SUCCESS";
    outJson["message"] = "SUCCESS";
    callback(null, outJson);   
}

function execGetSyncDtl(methodParam) {
    return new Promise(function (resolve, reject) {
        getSyncDtl( methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getSyncDtl(redirectParam,callback) {
    let coIdn = redirectParam.coIdn;
    let fileIdn = redirectParam.fileIdn || '';
    let portal = redirectParam.portal || '';
    let process = redirectParam.process || '';
    let refresh_min = redirectParam.refresh_min || '';
    let scheduleYN = redirectParam.scheduleYN || "N";
    let poolName = redirectParam.poolName;
    let days = redirectParam.days || '';
    let updates_min = redirectParam.updates_min || '';
    let minutes =  redirectParam.minutes || '';
    let service_url = redirectParam.service_url;
    let source = redirectParam.source;
    let prefix = redirectParam.prefix || '';
    var outJson={};

    var poolsList= require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || 'TPOOL';
    if(pool !=''){
        coreDB.getTransPoolConnect(pool,async function(error,tpoolconn){
            if(error){
                console.log(error);
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Get Conection!";
                callback(null,outJson);
            }else{
                console.log("new connection")
                let methodParam = {};
                methodParam["coIdn"] = coIdn;
                methodParam["fileIdn"] = fileIdn;
                methodParam["process"] = process;
                methodParam["portal"] = portal;
                methodParam["minutes"] = minutes;
                methodParam["days"] = days;
                let fileResult = await execGetStockFile(methodParam,tpoolconn);
                if(fileResult.status == 'SUCCESS'){
                    let fileObj = fileResult["result"] || {};
                    let username = fileObj["username"];
                    let password = fileObj["password"];
                    let filePath = fileObj["filePath"] || '';
                    let filename = fileObj["filename"] || '';
                    let deletePacketList = fileObj["deletePacketList"] || [];
                    let statusMap = fileObj["statusMap"] || {};
                    let packetDetails = fileObj["packetDetails"] || []; 
                    let formatNme = fileObj["formatNme"] || '';
                    var cachedUrl = require('qaq-core-util').cachedUrl;
                    let dbmsDtldata = await coreUtil.getCache(prefix+"dbms_"+coIdn,cachedUrl); 
                    dbmsDtldata = JSON.parse(dbmsDtldata); 
                    let files_url = dbmsDtldata["files_url"] || '';

                    let now = new Date();
                    now.setHours(now.getHours() + 5);
                    now.setMinutes(now.getMinutes() + 30);
                    let dte=dateFormat(now, "ddmmmyyyy hh:MM TT");
                    let subject = "";


                    if(portal == 'marketd'){
                        methodParam = {};
                        methodParam["username"] =username;
                        methodParam["password"] = password;
                        methodParam["filePath"] = filePath;
                        methodParam["filename"] = filename;
                        methodParam["coIdn"] = coIdn;
                        methodParam["process"] = process;
                        methodParam["deletePacketList"] = deletePacketList;
                        methodParam["statusMap"] = statusMap;
                        methodParam["service_url"] = service_url;
                        let syncResult = await execGetMarketSync(methodParam,tpoolconn);
                    } else if(portal == 'polygon'){
                        methodParam = {};
                        methodParam["username"] =username;
                        methodParam["password"] = password;
                        methodParam["filePath"] = filePath;
                        methodParam["filename"] = filename;
                        methodParam["coIdn"] = coIdn;
                        methodParam["process"] = process;
                        methodParam["service_url"] = service_url;
                        let syncResult = await execGetPolygonSync(methodParam,tpoolconn);
                    } else if(portal == 'getd'){
                        methodParam = {};
                        methodParam["apikey"] =username;
                        methodParam["packetDetails"] = packetDetails;
                        methodParam["coIdn"] = coIdn;
                        methodParam["process"] = process;
                        methodParam["deletePacketList"] = deletePacketList;
                        methodParam["service_url"] = service_url;
                        methodParam["username"] =username;
                        methodParam["password"] = password;
                        methodParam["filePath"] = filePath;
                        methodParam["filename"] = filename;
                        let syncResult = await execGetDimondSync(methodParam,tpoolconn);
                    } else if(portal == 'uni'){
                        methodParam = {};
                        methodParam["username"] =username;
                        methodParam["password"] = password;
                        methodParam["filePath"] = filePath;
                        methodParam["filename"] = filename;
                        methodParam["coIdn"] = coIdn;
                        methodParam["process"] = process;
                        methodParam["service_url"] = service_url;
                        let syncResult = await execGetUniSync(methodParam,tpoolconn);
                    } else if(portal == 'bn'){
                        subject = "Blue Nile File "+dte; 
                        methodParam = {};
                        methodParam["username"] =username;
                        methodParam["password"] = password;
                        methodParam["filePath"] = filePath;
                        methodParam["filename"] = filename;
                        methodParam["coIdn"] = coIdn;
                        methodParam["process"] = process;
                        methodParam["service_url"] = service_url;
                        let syncResult = await execGetBncSync(methodParam,tpoolconn);
                    } else if(portal == 'r2net'){
                        subject = "R2Net File "+dte; 
                        methodParam = {};
                        methodParam["username"] =username;
                        methodParam["password"] = password;
                        methodParam["filePath"] = filePath;
                        methodParam["filename"] = filename;
                        methodParam["coIdn"] = coIdn;
                        methodParam["process"] = process;
                        methodParam["service_url"] = service_url;
                        let syncResult = await execGetR2NetSync(methodParam,tpoolconn);
                    } 
                    if(formatNme != ''){                       
                        console.log("In Mail");
                        let cc = [];
                        let bcc = [];
                        let emailIds = [];
                        let methodParam = {};
                        methodParam["formatNme"]=formatNme;
                        methodParam["coIdn"]=coIdn;
                        methodParam["filename"]=filename;
                        methodParam["source"]=source;
                        methodParam["subject"]=subject;
                        methodParam["cc"]=cc; 
                        methodParam["bcc"]=bcc; 
                        methodParam["emailIds"]=emailIds; 
                        methodParam["message"]="Thank You.";
                        methodParam["files_url"] = files_url;
                        //console.log("mailSend");
                        let mailDetails = await execSendFTPMail(methodParam,tpoolconn);
                    } 

                    methodParam = {};
                    methodParam["fileIdn"] =fileIdn;
                    methodParam["refresh_min"] = refresh_min;
                    methodParam["scheduleYN"] = scheduleYN;
                    methodParam["process"] = process;
                    methodParam["updates_min"] = updates_min;
                    let intervalResult = await execUpdateInterval(methodParam,tpoolconn);
                    coreDB.doTransRelease(tpoolconn);
                    console.log("connection release");
                    callback(null,outJson);       
                } else {
                    coreDB.doTransRelease(tpoolconn);
                    console.log("connection release");
                    callback(null,fileResult);  
                }  
            }
        })
    } else{
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
        callback(null,outJson);
    }     
}

function execGetMarketSync(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getMarketSync(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getMarketSync(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var filePath = redirectParam.filePath;
    let process = redirectParam.process;
    let username = redirectParam.username;
    let password = redirectParam.password;
    let filename = redirectParam.filename;
    let deletePacketList = redirectParam.deletePacketList || [];
    let statusMap = redirectParam.statusMap || {};
    let service_url = redirectParam.service_url;
    var methodParam={};  
    var outJson={};

    let jwt = '';
    let clientSecret = '';
    methodParam = {};
    methodParam["username"] = username;
    methodParam["password"] = password;
    methodParam["service_url"] = service_url;
    let authResult = await execGetAuthenticate(methodParam);
    if(authResult.status == 'SUCCESS'){
        let tokenResult = authResult["result"] || {};

        let data = tokenResult["data"] || {};
        let token = data["token"] || {};
        jwt = token["jwt"] || ''; 
        clientSecret = data["clientSecret"] || '';
    } else {
        callback(null,authResult);       
    }
    if(process == 'refresh'){
        methodParam = {};
        methodParam["jwt"] =jwt;
        methodParam["clientSecret"] = clientSecret;
        methodParam["filePath"] = filePath;
        methodParam["filename"] = filename;
        methodParam["service_url"] = service_url;
        let uploadResult = await execGetUploadFile(methodParam);
        if(uploadResult.status == 'SUCCESS'){ 
            outJson["result"]=uploadResult["result"];
            outJson["status"]="SUCCESS";
            outJson["message"]="File Uploaded Successfully!"; 
            callback(null,outJson);   
        } else {
            callback(null,uploadResult);  
        }
    } else if(process == 'status'){
        var statusKeys=Object.keys(statusMap) || [];
        var statusKeyslen=statusKeys.length;
        if(statusKeyslen > 0){
            for(let i=0;i<statusKeyslen;i++){
                let status = statusKeys[i];
                console.log("status",status);
                let packetlist = statusMap[status] || [];
                console.log("packetlist",packetlist);
                if(packetlist.length > 0){
                    let stoneListStr = packetlist.toString();

                    methodParam = {};
                    methodParam["jwt"] =jwt;
                    methodParam["clientSecret"] = clientSecret;
                    methodParam["stoneListStr"] = stoneListStr;
                    methodParam["status"] = status;
                    methodParam["service_url"] = service_url;
                    let statusResult = await execGetMarketDUpdateStatus(methodParam);
                }
            }
            outJson["status"]="SUCCESS";
            outJson["message"]="Stones Status Updated Successfully!";  
            callback(null,outJson); 
        }  else {
            outJson["status"] = "FAIL";
            outJson["message"] = "Sorry no result found for status updation";
            callback(null, outJson);
        }
    }  else if(process == 'delete'){
        if(deletePacketList.length > 0){
            let stoneListStr = deletePacketList.toString();
            methodParam = {};
            methodParam["jwt"] =jwt;
            methodParam["clientSecret"] = clientSecret;
            methodParam["stoneListStr"] = stoneListStr;
            methodParam["service_url"] = service_url;
            let deleteResult = await execGetMarketDeletePkt(methodParam);
            if(deleteResult.status == 'SUCCESS'){ 
                outJson["result"]=deleteResult["result"];
                outJson["status"]="SUCCESS";
                outJson["message"]="Stones Deleted Successfully!";  
                callback(null,outJson);  
            } else {
                callback(null,deleteResult);  
            }
        }  else {
            callback(null, outJson);
        }
    }
}

function execGetPolygonSync(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getPolygonSync(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getPolygonSync(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var filePath = redirectParam.filePath;
    let process = redirectParam.process;
    let username = redirectParam.username;
    let password = redirectParam.password;
    let filename = redirectParam.filename;
    let service_url = redirectParam.service_url;
    var methodParam={};  
    var outJson={};

    if(process == 'refresh'){
        methodParam = {};
        methodParam["password"] = password;
        methodParam["username"] = username;
        methodParam["filePath"] = filePath;
        methodParam["filename"] = filename;
        methodParam["service_url"] = service_url;
        getUploadFtpFile(methodParam);
        
        outJson["status"]="SUCCESS";
        outJson["message"]="File Uploaded Successfully!"; 
        callback(null,outJson);     
    } 
}

function execGetR2NetSync(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getR2NetSync(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getR2NetSync(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var filePath = redirectParam.filePath;
    let process = redirectParam.process;
    let username = redirectParam.username;
    let password = redirectParam.password;
    let filename = redirectParam.filename;
    let service_url = redirectParam.service_url;
    var methodParam={};  
    var outJson={};

    if(process == 'refresh'){
        methodParam = {};
        methodParam["password"] = password;
        methodParam["username"] = username;
        methodParam["filePath"] = filePath;
        methodParam["filename"] = filename;
        methodParam["service_url"] = service_url;
        getUploadFtpFile(methodParam);
        
        outJson["status"]="SUCCESS";
        outJson["message"]="File Uploaded Successfully!"; 
        callback(null,outJson);     
    } 
}

function execGetDimondSync(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getDiamondSync(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getDiamondSync(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var packetDetails = redirectParam.packetDetails || [];
    let process = redirectParam.process;
    let apikey = redirectParam.apikey;
    let deletePacketList = redirectParam.deletePacketList || [];
    let service_url = redirectParam.service_url;
    var filePath = redirectParam.filePath;
    let username = redirectParam.username;
    let password = redirectParam.password;
    let filename = redirectParam.filename;
    var methodParam={};  
    var outJson={};

    if(process == 'refresh'){
       // methodParam = {};
       // methodParam["packetDetails"] = packetDetails;
       // methodParam["apikey"] = apikey;
       // methodParam["service_url"] = service_url;
       // let syncResult = await execGetUploadDiamondFile(methodParam);
        methodParam = {};
        methodParam["password"] = password;
        methodParam["username"] = username;
        methodParam["filePath"] = filePath;
        methodParam["filename"] = filename;
        methodParam["service_url"] = service_url;
        methodParam["remotePath"] = '/home/KapuGemsLtd231/'
        getUploadFtpFile(methodParam);
        
        outJson["status"]="SUCCESS";
        outJson["message"]="File Uploaded Successfully!"; 
        callback(null,outJson);     
    } else if(process == 'delete'){
        for(let i=0;i<deletePacketList.length;i++){
            let stockIdn = deletePacketList[i];
            methodParam = {};
            methodParam["apikey"] =apikey;
            methodParam["stockIdn"] = stockIdn;
            methodParam["service_url"] = service_url;
            let deleteResult = await execGetDiamondDeletePkt(methodParam);
        }
       
        outJson["status"]="SUCCESS";
        outJson["message"]="Stones Deleted Successfully!";  
        callback(null,outJson);  
    } else {
        callback(null,outJson);  
    }
}

function execGetUniSync(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getUniSync(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getUniSync(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var filePath = redirectParam.filePath;
    let process = redirectParam.process;
    let username = redirectParam.username;
    let password = redirectParam.password;
    let filename = redirectParam.filename;
    let service_url = redirectParam.service_url;
    var methodParam={};  
    var outJson={};

    if(process == 'refresh'){
        methodParam = {};
        methodParam["password"] = password;
        methodParam["username"] = username;
        methodParam["filePath"] = filePath;
        methodParam["filename"] = filename;
        methodParam["service_url"] = service_url;
        getUploadFtpFile(methodParam);
        
        outJson["status"]="SUCCESS";
        outJson["message"]="File Uploaded Successfully!"; 
        callback(null,outJson);     
    } 
}

function execGetBncSync(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getBncSync(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getBncSync(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var filePath = redirectParam.filePath;
    let process = redirectParam.process;
    let username = redirectParam.username;
    let password = redirectParam.password;
    let filename = redirectParam.filename;
    let service_url = redirectParam.service_url;
    var methodParam={};  
    var outJson={};

    if(process == 'refresh'){
        methodParam = {};
        methodParam["password"] = password;
        methodParam["username"] = username;
        methodParam["filePath"] = filePath;
        methodParam["filename"] = filename;
        methodParam["service_url"] = service_url;
        getUploadFtpFile(methodParam);
        
        outJson["status"]="SUCCESS";
        outJson["message"]="File Uploaded Successfully!"; 
        callback(null,outJson);     
    } 
}

function execGetStockFile(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getStockFile(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function getStockFile(tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var fileIdn = redirectParam.fileIdn || '';
    let process = redirectParam.process;
    let portal = redirectParam.portal;
    let days = redirectParam.days || '';
    let minutes = redirectParam.minutes || '';
    var paramJson={};  
    var outJson={};
    var now = new Date();
    var dte=dateFormat(now, "ddmmmyyyy_hMMss");
    var nowPol = new Date();
    var dtePol=dateFormat(nowPol,"yyyymmdd");
    let resultFinal = {};

    if(fileIdn != ''){ 
        paramJson={};    
        paramJson["fileIdn"]=fileIdn;
        paramJson["coIdn"]=coIdn;
        let fileOptionResult = await execGetFileOptionsDtl(paramJson,tpoolconn);
        if(fileOptionResult.status == 'SUCCESS'){
            let fileOptionDtl = fileOptionResult.result || {};
            let fileoptionname = fileOptionDtl.fileoptionname || '';
            let filename = fileOptionDtl["filename"] || '';
            let fileExtension = fileOptionDtl["fileExtension"] || 'csv';
            let key_mapping = fileOptionDtl["key_mapping"];
            fileIdn = fileOptionDtl["file_idn"];
            let addl_attr = fileOptionDtl["searchattr"] || '';
            resultFinal["username"] = fileOptionDtl["username"];
            resultFinal["password"] = fileOptionDtl["password"];
            resultFinal["formatNme"] = fileOptionDtl["mailformat"] || '';

            if(process == 'refresh'){
                if(filename.indexOf("~datetime~") > -1){
                    if(portal == 'polygon'){
                        filename = replaceall("~datetime~",dtePol, filename);
                    } else {
                        filename = replaceall("~datetime~",dte, filename);
                    }   
                } //else if(portal != 'bn') {
                  //  filename = filename+"_"+dte;
               // }
                 
                paramJson={};    
                paramJson["fileIdn"]=fileIdn;
                paramJson["filemap"]=key_mapping;
                paramJson["addl_attr"]=addl_attr;
                paramJson["fileoptionname"] = fileoptionname;
                paramJson["coIdn"]=coIdn;
                let fileArrayResult = await execGenFileProcedure(paramJson,tpoolconn);
                //fileArrayResult["status"] = "SUCCESS";
                if(fileArrayResult.status == 'SUCCESS'){
                    let resultView = fileArrayResult["resultView"] || [];
                    let packetDetails = fileArrayResult["packetDetails"] || [];
                    let resultViewlen = resultView.length;
                    let attrDataType = {};

                    let packetDtlList = [];
                    for(let i=0;i<packetDetails.length;i++){
                        let pktdtl = packetDetails[i] || [];
                        let packetDtl = {};
                        for(let j=0;j<resultViewlen;j++){
                            let attr = resultView[j];
                            let attrVal = pktdtl[j] || '';
                            packetDtl[attr] = attrVal;
                        }
                        packetDtlList.push(packetDtl);
                    }
                    
                    //console.log("fileExtension",fileExtension)
                    //console.log("filename",filename)
                    let filePath = "";
                    if(fileExtension == 'excel'){
                        filename = filename+'.xlsx';
                        filePath = "files/"+filename;
                        paramJson = {};
                        paramJson["resultView"] = resultView;
                        paramJson["pktDetailsList"] = packetDtlList;
                        paramJson["attrDataType"] = attrDataType;
                        paramJson["filename"] = filename;
                        let excelResult = await execSaveExcel(paramJson,tpoolconn);
                        if(excelResult.status == 'SUCCESS'){
                            resultFinal["filename"] = filename;
                            resultFinal["filePath"] = filePath;
                            outJson["result"]=resultFinal;
                            outJson["status"]="SUCCESS";
                            outJson["message"]="SUCCESS";
                            callback(null,outJson);
                        } else {
                            callback(null,excelResult);
                        }
                    } else if(fileExtension == 'csv'){
                        filename = filename+'.csv';   
                        filePath = "files/"+filename;               
                        const json2csvParser = new Json2csvParser({ resultView });
                        const csv = json2csvParser.parse(packetDtlList);
                        //console.log(csv)
                        //now = new Date();
                        //var dtes=dateFormat(now, "ddmmmyyyy_hMMss");
                        //console.log("start",dtes);
                        let writerStream = fs.createWriteStream('files/'+filename);
                        writerStream.write(csv,'UTF8'); //writeFile
                        writerStream.end();
                        //fs.writeFile('files/'+filename, csv,async function(err) {
                        //    if (err) {
                        //            console.log("error",err)
                        //            outJson["result"]=resultFinal;
                        //            outJson["status"]="FAIL";
                        //            outJson["message"]="CSV Download Fail";
                        //            callback(null,outJson);
                        //        }
                        //console.log("end");
                        //now = new Date();
                        //var dtef=dateFormat(now, "ddmmmyyyy_hMMss");
                        //console.log("end file",dtef);

                        resultFinal["filename"] = filename;
                        resultFinal["filePath"] = filePath;
                        outJson["result"]=resultFinal;
                        outJson["status"]="SUCCESS";
                        outJson["message"]="SUCCESS";
                        callback(null,outJson);
                    //})
                    } else {
                        console.log("packetLength",packetDtlList.length)
                        resultFinal["packetDetails"] = packetDtlList;
                        outJson["result"]=resultFinal;
                        outJson["status"]="SUCCESS";
                        outJson["message"]="SUCCESS";
                        callback(null,outJson);
                    }                                                   
                } else {
                    callback(null,fileArrayResult);
                }
            } else if(process == 'delete') {
                paramJson={};    
                paramJson["coIdn"]=coIdn;
                paramJson["days"]=days;
                let deleteResult = await execDeletePacketsProcedure(paramJson,tpoolconn);
                if(deleteResult.status == 'SUCCESS'){
                    let deletePacketList = deleteResult["deletePacketList"] || [];
                    resultFinal["deletePacketList"] = deletePacketList;

                    outJson["result"]=resultFinal;
                    outJson["status"]="SUCCESS";
                    outJson["message"]="SUCCESS";
                    callback(null,outJson);   
                } else {
                    callback(null,deleteResult);
                }
            } else if(process == 'status') {
                paramJson={};    
                paramJson["coIdn"]=coIdn;
                paramJson["minutes"]=minutes;
                let statusResult = await execPacketStatusProcedure(paramJson,tpoolconn);
                if(statusResult.status == 'SUCCESS'){
                    let statusMap = statusResult["statusMap"] || {};
                    //console.log("statusMap",statusMap);

                    resultFinal["statusMap"] = statusMap;

                    outJson["result"]=resultFinal;
                    outJson["status"]="SUCCESS";
                    outJson["message"]="SUCCESS";
                    callback(null,outJson);   
                } else {
                    callback(null,statusResult);
                }
            }     
        } else {
            callback(null,fileOptionResult);
        }   
    } else if(fileIdn == ''){
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify FileIdn Parameter";
        callback(null,outJson);
    } 
}

function execGenFileProcedure(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        genFileProcedure(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function genFileProcedure(tpoolconn, paramJson, callback) {
    var fileIdn = paramJson.fileIdn || '';
    var filemap = paramJson.filemap || '';
    let addl_attr = paramJson.addl_attr || '';
    let fileoptionname = paramJson.fileoptionname || '';
    let coIdn = paramJson.coIdn || '';

    let outJson = {};
    let list = [];

    if (fileIdn != '') {
        let params = [];
        let fmt = {};
        let query = "select gen_file_ary($1,$2,$3) filearry";
        params.push(fileIdn);
        params.push(filemap);
        params.push(addl_attr);

        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt,async function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "gen_file_ary Fail To Execute Query!";
                callback(null, outJson);
            } else {
                let rowCount = result.rowCount;
                if (rowCount > 0) {
                    var len = result.rows.length;
                    //console.log("file Packet Len"+len);
                    let resultView = result.rows[0].filearry;
                    //console.log("resultView"+resultView);
                    for (let i = 1; i < len; i++) {
                        let rows = result.rows[i];
                        let obj  = rows["filearry"];
                        list.push(obj);
                    }

                    let tpData = 'N';
                    if(fileoptionname == 'r2net'){
                        let methodParam = {};
                        methodParam["coIdn"] = coIdn;
                        let applData = await execGetApplDtl(methodParam,tpoolconn);
                        if(applData.status == 'SUCCESS'){
                            tpData = applData["result"] || 'N';
                        }
                    }
                    //console.log("tp",tpData);
                    if(tpData == 'Y'){
                        let packetList = [];
                        let methodParam = {};
                        methodParam["fileNme"] = fileoptionname;
                        methodParam["packetList"] = list;
                        let tpDataResult = await execGetTPFileOptionsDtl(methodParam,tpoolconn);
                        if(tpDataResult.status == 'SUCCESS'){
                            packetList = tpDataResult["packetDetails"];
                            outJson["packetDetails"] = packetList;
                        } else {
                            outJson["packetDetails"] = list;
                        }

                        outJson["resultView"] = resultView;
                        outJson["status"] = "SUCCESS";
                        outJson["message"] = "SUCCESS";
                        callback(null, outJson);
                    } else {
                        outJson["resultView"] = resultView;
                        outJson["packetDetails"] = list;
                        outJson["status"] = "SUCCESS";
                        outJson["message"] = "SUCCESS";
                        callback(null, outJson);
                    } 
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Sorry no result found";
                    callback(null, outJson);
                }
            }
        });
    } else if (fileIdn == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify File Idn Parameter";
        callback(null, outJson);
    }
}

function execGetTPFileOptionsDtl(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getTPFileOptionsDtl(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function getTPFileOptionsDtl(tpoolconn, paramJson, callback) {
    let fileNme = paramJson.fileNme || '';
    let packetList = paramJson.packetList || [];
    let outJson = {};
    let map = {};

    if (fileNme != '') {
        let params = [];
        let fmt = {};
        let query = "select fo.key_mapping ,fo.file_idn, fo.addl_attr->> 'searchattr' searchattr \n"+
            "from file_options fo,appl_client ac where fo.co_idn = ac.co_idn \n"+
            " and ac.account_ds = 'TP' \n"+
            "and fo.stt = 1 and ac.stt = 1 ";

        let cnt =0;
        if(fileNme != ''){
            cnt++;
            query +=" and fo.nme=$"+cnt;
            params.push(fileNme);
        }
        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt,async function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "getTPFileOptionsDtl Fail To Execute Query!";
                callback(null, outJson);
            } else {
                var len = result.rows.length;
                if (len > 0) {
                    let filemap = result.rows[0].key_mapping;
                    let fileIdn = result.rows[0].file_idn;
                    let addl_attr = result.rows[0].searchattr || '';

                    params = [];
                    fmt = {};
                    query = "select gen_file_ary($1,$2,$3) filearry";
                    params.push(fileIdn);
                    params.push(filemap);
                    params.push(addl_attr);

                    //console.log(query);
                    //console.log(params);
                    coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
                        if (error) {
                            console.log(error);
                            outJson["result"] = '';
                            outJson["status"] = "FAIL";
                            outJson["message"] = "gen_file_ary Fail To Execute Query!";
                            callback(null, outJson);
                        } else {
                            let rowCount = result.rowCount;
                            if (rowCount > 0) {
                                var len = result.rows.length;
                                console.log("file Packet Len"+len);
                                let resultView = result.rows[0].filearry;
                                //console.log("resultView"+resultView);
                                for (let i = 1; i < len; i++) {
                                    let rows = result.rows[i];
                                    let obj  = rows["filearry"];
                                    packetList.push(obj);
                                }
                                outJson["resultView"] = resultView;
                                outJson["packetDetails"] = packetList;
                                outJson["status"] = "SUCCESS";
                                outJson["message"] = "SUCCESS";
                                callback(null, outJson);
                            } else {
                                outJson["status"] = "FAIL";
                                outJson["message"] = "Sorry no result found";
                                callback(null, outJson);
                            }
                        }
                    });
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Sorry no result found";
                    callback(null, outJson);
                }
            }
        });
    } else if (fileNme == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify FileName Parameter";
        callback(null, outJson);
    }
}

function execDeletePacketsProcedure(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        deletePacketsProcedure(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function deletePacketsProcedure(tpoolconn, paramJson, callback) {
    let coIdn = paramJson.coIdn;
    let days = paramJson.days;
    let outJson = {};
    let list = [];

    if (days != '') {
        let params = [];
        let fmt = {};
        let query = "select get_portal_delete($1, $2) packetlist";
        params.push(coIdn);
        params.push(days);

        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "get_portal_delete Fail To Execute Query!";
                callback(null, outJson);
            } else {
                let rowCount = result.rows.length;
                if (rowCount > 0) {
                    let deletePacketList = result.rows[0].packetlist;
        
                    outJson["deletePacketList"] = deletePacketList;
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "SUCCESS";
                    callback(null, outJson);
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Sorry no result found";
                    callback(null, outJson);
                }
            }
        });
    } else if (days == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify Days Parameter";
        callback(null, outJson);
    }
}

function execPacketStatusProcedure(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        packetStatusProcedure(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function packetStatusProcedure(tpoolconn, paramJson, callback) {
    let coIdn = paramJson.coIdn;
    let minutes = paramJson.minutes;
    let outJson = {};

    if (minutes != '') {
        let params = [];
        let fmt = {};
        let query = "select get_portal_update($1, $2) packetlist";
        params.push(coIdn);
        params.push(minutes);

        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "get_portal_update Fail To Execute Query!";
                callback(null, outJson);
            } else {
                let rowCount = result.rows.length;
                if (rowCount > 0) {
                    let statusMap = {};
                    var packetlist = result.rows[0].packetlist || [];
                    for (let i = 0; i < packetlist.length; i++) {
                        let rows = packetlist[i];
                        let status  = rows["status"];
                        let pkt_code = rows["pkt_code"];
                        let list = statusMap[status] || [];
                        list.push(pkt_code);
                        statusMap[status] = list;
                    }
                    outJson["statusMap"] = statusMap;
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "SUCCESS";
                    callback(null, outJson);
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Sorry no result found";
                    callback(null, outJson);
                }
            }
        });
    } else if (minutes == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify Minutes Parameter";
        callback(null, outJson);
    }
}

function execGetFileOptionsDtl(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getFileOptionsDtl(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function getFileOptionsDtl(tpoolconn, paramJson, callback) {
    var fileIdn = paramJson.fileIdn || '';
    let fileNme = paramJson.fileNme || '';
    var coIdn = paramJson.coIdn;
    let outJson = {};
    let map = {};

    if (fileIdn != '' || fileNme != '') {
        let params = [];
        let fmt = {};
        let query = "select nme,addl_attr->> 'filename' filename, "+
            "addl_attr->> 'fileExtension' fileExtension, "+
            "addl_attr->> 'searchattr' searchattr, "+
            "addl_attr->> 'username' username, "+
            "addl_attr->> 'password' passwords, "+
            "addl_attr->> 'mailformat' mailformat, "+
            "key_mapping ,file_idn "+					  
            "from file_options  where co_idn=$1 and stt=1 ";

        params.push(coIdn);
        let cnt =1;
        if(fileIdn != ''){
            cnt++;
            query +=" and file_idn=$"+cnt;
            params.push(fileIdn);
        }
        if(fileNme != ''){
            cnt++;
            query +=" and nme=$"+cnt;
            params.push(fileNme);
        }
        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt,async function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "getFileOptionsDtl Fail To Execute Query!";
                callback(null, outJson);
            } else {
                var len = result.rows.length;
                if (len > 0) {
                    let nme = result.rows[0].nme;
                    map["fileoptionname"] =  nme;
                    map["filename"] = result.rows[0].filename;
                    map["fileExtension"] = result.rows[0].fileextension;
                    map["key_mapping"] = result.rows[0].key_mapping;
                    map["file_idn"] = result.rows[0].file_idn;
                    map["mailformat"] = result.rows[0].mailformat || '';
                    map["searchattr"] = result.rows[0].searchattr || '';
                    map["username"] = result.rows[0].username || '';
                    map["password"] = result.rows[0].passwords || '';

                    outJson["result"] = map;
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "SUCCESS";
                    callback(null, outJson);
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Sorry no result found";
                    callback(null, outJson);
                }
            }
        });
    } else if (fileIdn == '' || fileNme == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify FileIdn/FileName Parameter";
        callback(null, outJson);
    }
}

function execGetApplDtl(methodParam,tpoolconn){
    return new Promise(function(resolve,reject) {
        getApplDtl(tpoolconn,methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function getApplDtl(connection,paramJson,callback){
    let coIdn = paramJson.coIdn;
    let outJson = {};

    let params = [];
    params.push(coIdn);
    let fmt = {};
    let query = "select appl_idn from appl_client where co_idn=$1 and account_ds like 'GD%' \n";
    
    //console.log(query);
    //console.log(params)
    coreDB.executeTransSql(connection,query,params,fmt,function(error,result){
        if(error){
            console.log(error);
            outJson["result"]='';
            outJson["status"]="FAIL";
            outJson["message"]="getApplDtl Fail To Execute Query!"+error;
            callback(null,outJson);
        }else{
            var len = result.rows.length;
            //console.log("len",len)
            if(len>0){
                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                outJson["result"]='Y';
                callback(null,outJson);
            } else {
                outJson["status"]="FAIL";
                outJson["message"]="Sorry no result found";
                callback(null,outJson);
            }
        }
    })                 
}

function execSaveExcel(methodParam, connection) {
    return new Promise(function (resolve, reject) {
        saveExcel(connection, methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function saveExcel(connection,paramJson,callback){
    var pktDetailsList = paramJson.pktDetailsList || '';
    var resultView = paramJson.resultView;
    let attrDataType = paramJson.attrDataType || {};
    let fileName = paramJson.filename;
    var outJson = {};
    var workbook = new excel.Workbook();
    let totalPcs = pktDetailsList.length;
    let resultViewlen = resultView.length;
   
    var worksheet = workbook.addWorksheet('Sheet 1');

    var style2 = workbook.createStyle({
        font: {
            color: '00000000',
            name: 'Calibri',
            size: 11
        },
        alignment: {
            wrapText: true,
            horizontal: 'center',
        }
             
    });

    var headerVals = 1;
    var rowVals = headerVals + 1;

    var cnt = 0;
    var columnmList={};
    for(let i=0;i<resultViewlen;i++){
        var attr = resultView[i];
        var vals = 1+cnt;

        worksheet.cell(headerVals,vals).string(attr).style(style2);
        let maxLen = attr.toString().length+3;
        columnmList[vals]=parseInt(maxLen);

        cnt++;
    }

    for(let i=0;i<totalPcs;i++){
        let packetDtls = pktDetailsList[i] || [];
        var cnts=0;
        for(var k=0;k<resultViewlen;k++){
            var attr = resultView[k];
            var dataTyp = attrDataType[k] || 't';
            var vals = 1+cnts;
            let attrVal = packetDtls[attr] || '';
            attrVal =coreUtil.nvl(attrVal,'');

            if(attrVal == 'null' || attrVal=='NA' || attrVal=='na')
                attrVal='';

            if(dataTyp!='n' && dataTyp!='f'){
                    worksheet.cell(rowVals,vals).string(attrVal).style(style2);
            }else{
                if(attrVal=='')
                    worksheet.cell(rowVals,vals).string(attrVal).style(style2);
                else
                    worksheet.cell(rowVals,vals).number(parseFloat(attrVal)).style(style2);    
            }

            if(attrVal != ''){
                let curLent = columnmList[vals]||'';
                let colmaxLen = parseInt(attrVal.toString().length+3);
                let maxLen =  Math.max(curLent, colmaxLen);

                columnmList[vals]=maxLen;
            }
            cnts++;
        }   
        rowVals++;
    }
   
    var colLst=Object.keys(columnmList) ;
    for(let i=0;i < colLst.length;i++){
           var col = parseInt(colLst[i]);
           var maxWd = columnmList[col]||10;
              worksheet.column(col).setWidth(maxWd);
    }
    workbook.write('files/'+fileName, function(err, stats) {
        if (err) {
            console.error(err);
            outJson["result"]='';
            outJson["status"]="FAIL";
            outJson["message"]="Excel Save Failed!";
            callback(null,outJson);
        } else {
            outJson["result"]=fileName;
            outJson["status"]="SUCCESS";
            outJson["message"]="Excel Saved Successfully!";
            callback(null,outJson);
        }
    })
}

function execGetAuthenticate(methodParam) {
    return new Promise(function (resolve, reject) {
        getAuthenticate( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getAuthenticate(paramJson, callback){
    let username = paramJson.username || '';
    let password = paramJson.password || '';
    let service_url = paramJson.service_url;
    let outJson = {};
    let resultFinal = {};
    let authData = {};
    authData["username"] = username;
    authData["password"] = password;

    var headers = {
        'Content-Type':'application/json'
    }
    //console.log(authData)

    var options = {
        url: service_url+'/auth/login',
        method: 'POST',
        headers: headers,
        form: authData
    };
    //console.log(options);
    request(options,async function (error, response, body) {
        //console.log(error);
        //console.log("statusCode"+response.statusCode );
        //console.log(response );
        //console.log(response.message );
        if (!error && response.statusCode == 200) {
            //console.log("body"+body); // Print the shortened url.
            let info = JSON.parse(body);
            console.log(info);
            outJson["result"] = info;
            outJson["message"]="SUCCESS";
            outJson["status"]="SUCCESS";
            callback(null,outJson);        
        }else{

            outJson["message"]=error;
            outJson["status"]="FAIL";
            //console.log("IN",error);
            callback(null,outJson);   
        }
    });   
}

function execGetUploadFile(methodParam) {
    return new Promise(function (resolve, reject) {
        getUploadFile( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getUploadFile(paramJson, callback){
    let filePath = paramJson.filePath || '';
    let clientSecret = paramJson.clientSecret || '';
    let jwt = paramJson.jwt || '';
    let filenme = paramJson.filename || '';
    let service_url = paramJson.service_url;
    let outJson = {};

    let formData = {
        file: {
            value: fs.createReadStream(filePath),
            options: {
                filename: filenme
            }
        }
    }

    var headers = {
        'Authorization':"JWT " + jwt,
        'UploadType':'OVERWRITE',
        'ContentType':'multipart/form-data'
    }

    var options = {
        url: service_url+'/diamond/'+clientSecret+'/upload-sheet',
        method: 'POST',
        headers: headers,
        formData: formData 
    };
    
    //console.log(options);
    request(options,async function (error, response, body) {
        //console.log(error);
        //console.log("statusCode"+response.statusCode );
        //console.log(response );
        //console.log(response.message );
        //console.log(body);
        if (!error && response.statusCode == 200) {
            let info = JSON.parse(body);
            console.log(info);
            outJson["result"] = info;
            outJson["message"]="SUCCESS";
            outJson["status"]="SUCCESS";
            callback(null,outJson);        
        }else{
            outJson["message"]="Issue in API";
            outJson["status"]="FAIL";
            callback(null,outJson);   
        }
    }); 
}

function execGetMarketDUpdateStatus(methodParam) {
    return new Promise(function (resolve, reject) {
        getMarketDUpdateStatus( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getMarketDUpdateStatus(paramJson, callback){
    let stoneListStr = paramJson.stoneListStr || '';
    let clientSecret = paramJson.clientSecret || '';
    let jwt = paramJson.jwt || '';
    let status = paramJson.status || '';
    let service_url = paramJson.service_url;
    let outJson = {};
    let formData = {};
    formData["vStnId"] = stoneListStr;
    formData["status"] = status;

    var headers = {
        'Authorization':"JWT " + jwt,
        'ContentType':'application/json'
    }
    //console.log("formData",formData);
    var options = {
        url: service_url+'/diamond/'+clientSecret+'/update-status',
        method: 'POST',
        headers: headers,
        form: formData 
    };
    
    //console.log(options);
    request(options,async function (error, response, body) {
        //console.log(error);
        //console.log("statusCode"+response.statusCode );
        //console.log(response );
        //console.log(response.message );
        if (!error && response.statusCode == 200) {
            let info = JSON.parse(body);
            console.log(info);

            outJson["result"] = info;
            outJson["message"]="SUCCESS";
            outJson["status"]="SUCCESS";
            callback(null,outJson);        
       }else{
            outJson["result"] = {};
            outJson["message"]="Issue in API";
            outJson["status"]="FAIL";
            callback(null,outJson);   
        }
    }); 
}

function execGetMarketDeletePkt(methodParam) {
    return new Promise(function (resolve, reject) {
        getMarketDeletePkt( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getMarketDeletePkt(paramJson, callback){
    let stoneListStr = paramJson.stoneListStr || '';
    let clientSecret = paramJson.clientSecret || '';
    let jwt = paramJson.jwt || '';
    let service_url = paramJson.service_url;
    let outJson = {};
    let formData = {};
    formData["vStnId"] = stoneListStr;

    var headers = {
        'Authorization':"JWT " + jwt,
        'ContentType':'application/json'
    }

    var options = {
        url: service_url+'/diamond/'+clientSecret+'/delete',
        method: 'POST',
        headers: headers,
        form: formData 
    };
    
    //console.log(options);
    request(options,async function (error, response, body) {
        //console.log(error);
        //console.log("statusCode"+response.statusCode );
        //console.log(response );
        //console.log(response.message );
        let info = JSON.parse(body);
        console.log(info);
        if (!error && response.statusCode == 200) {
            outJson["result"] = info;
            outJson["message"]="SUCCESS";
            outJson["status"]="SUCCESS";
            callback(null,outJson);        
        }else{
            outJson["result"] = info;
            outJson["message"]="Issue in API";
            outJson["status"]="FAIL";
            callback(null,outJson);   
        }
    }); 
}

function execUpdateInterval(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        updateInterval(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateInterval(tpoolconn, paramJson, callback) {
    var fileIdn = paramJson.fileIdn || '';
    var refresh_min = paramJson.refresh_min || 0;
    let scheduleYN = paramJson.scheduleYN || "N";
    let process = paramJson.process;
    let updates_min = paramJson.updates_min || 0;
    let outJson = {};
    let list = [];

    if (fileIdn != '') {
        let params = [];
        let fmt = {};
        let conQ = '';
        let cnt = 0;
        let query = "";
        if(process == 'refresh') {
            //if(scheduleYN == "Y"){
                cnt++;
                conQ = ",next_refresh_ts =  current_timestamp + ($"+cnt+" ||' minutes')::interval \n";
                params.push(refresh_min);
            //}
            cnt++;
            query = "update file_options set last_refresh_ts=current_timestamp \n"+ conQ +
                "where file_idn = $"+cnt;
            
            params.push(fileIdn);
        } else {
            query = "update file_options set last_updates_ts=current_timestamp,\n"+
                "next_updates_ts=current_timestamp + ($1 ||' minutes')::interval \n"+
                "where file_idn = $2";

            params.push(updates_min);
            params.push(fileIdn);
        }
       

        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "Update Interval Fail To Execute Query!";
                callback(null, outJson);
            } else {
                let rowCount = result.rowCount;
                if (rowCount > 0) {
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "SUCCESS";
                    callback(null, outJson);
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Failed to update interval";
                    callback(null, outJson);
                }
            }
        });
    } else if (fileIdn == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify File Idn Parameter";
        callback(null, outJson);
    }
}

function getUploadFtpFile(paramJson){
    let filePath = paramJson.filePath || '';
    let filename = paramJson.filename || '';
    let username = paramJson.username || '';
    let password = paramJson.password || '';
    let service_url = paramJson.service_url;
    let remotePath = paramJson.remotePath || '/';

    const config = {
        host: service_url,
        port: 22,
        username: username,
        password: password
    };
      console.log(config)
    let sftp = new Client;
    
    let data = fs.createReadStream(filePath);
    let remote = remotePath+''+filename;
    console.log("remotePath",remote);
    sftp.connect(config)
    .then(() => {
        return sftp.put(data, remote);
    })
    .then(() => {
        return sftp.end();
    })
    .catch(err => {
        console.log(err.message);
    });
}

function execGetUploadDiamondFile(methodParam) {
    return new Promise(function (resolve, reject) {
        getUploadDiamondFile( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getUploadDiamondFile(paramJson, callback){
    let packetDetails = paramJson.packetDetails || [];
    let apikey = paramJson.apikey || '';
    let service_url = paramJson.service_url;
    let outJson = {};


    var headers = {
        'API-KEY':apikey,
        'Content-Type':'application/json'
    }

    var options = {
        url: service_url,
        method: 'POST',
        headers: headers,
        form: packetDetails 
    };
    
    //console.log(options);
    request(options,async function (error, response, body) {
        console.log("error",error);
        //console.log("statusCode"+response.statusCode );
            console.log(response );
        //console.log(response.message );
        let info = body;//JSON.parse(body);
        console.log(info);
        if (!error && response.statusCode == 200) {
            outJson["result"] = info;
            outJson["message"]="SUCCESS";
            outJson["status"]="SUCCESS";
            callback(null,outJson);        
        }else{
            outJson["result"] = info;
            outJson["message"]="Issue in API";
            outJson["status"]="FAIL";
            callback(null,outJson);   
        }
    }); 
}

function execGetDiamondDeletePkt(methodParam) {
    return new Promise(function (resolve, reject) {
        getDiamondDeletePkt( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getDiamondDeletePkt(paramJson, callback){
    let apikey = paramJson.apikey || '';
    let stockIdn = paramJson.stockIdn || '';
    let service_url = paramJson.service_url;
    let outJson = {};
    let list = [];
    let formData = {};
    formData["productId"] = stockIdn;
    list.push(formData);

    var headers = {
        'API-KEY':apikey,
        'Content-Type':'application/json'
    }

    var options = {
        url: service_url,
        method: 'DELETE',
        headers: headers,
        form: list 
    };
    
    //console.log(options);
    request(options,async function (error, response, body) {
        console.log(error);
        //console.log("statusCode"+response.statusCode );
        console.log(response );
       //console.log(response.message );
        let info = body;//JSON.parse(body);
        console.log(info);
        if (!error && response.statusCode == 200) {
            outJson["result"] = info;
            outJson["message"]="SUCCESS";
            outJson["status"]="SUCCESS";
            callback(null,outJson);        
        }else{
            outJson["result"] = info;
            outJson["message"]="Issue in API";
            outJson["status"]="FAIL";
            callback(null,outJson);   
        }
    }); 
}

exports.sendAutoReviseMail =function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    let outJson = {};

    let processNme = req.body.processNme || 'auto_revise';
    let intervalTime = req.body.intervalTime || '15';
    var poolsList = require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || '';
    if (pool != '') {
        coreDB.getTransPoolConnect(pool, async function (error, tpoolconn) {
            if (error) {
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "Fail To Get Conection!";
                callback(null, outJson);
            } else {
                //console.log("mail Sending")
                let methodParamlocal = {};
                methodParamlocal["processNme"] = processNme;
                methodParamlocal["coIdn"] = coIdn;
                methodParamlocal["source"] = source;
                methodParamlocal["intervalTime"] = intervalTime;
                let result = await coreUtil.execSendReviseMail(methodParamlocal, tpoolconn);
                coreDB.doTransRelease(tpoolconn);
                //console.log("new connection realese");
                outJson["result"] = '';
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            }
        })
    } else {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Fail To Get Conection!";
        callback(null, outJson);
    }
}

function execSendFTPMail(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        mailSendFTP(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

async function mailSendFTP(connection,paramJson,callback){
    var formatNme = paramJson.formatNme || '';
    var coIdn = paramJson.coIdn || '';
    var filename = paramJson.filename || '';
    var sal_source = paramJson.source || 'NA';
    var subjects = paramJson.subject || '';
    var ccId = paramJson.cc || [];
    var bccId = paramJson.bcc || [];
    var emailIds = paramJson.emailIds || [];
    var message =  paramJson.message || '';
    let files_url = paramJson.files_url || '';
    var outJson = {};
    let resultFinal = {};
    console.log("In Mail");

    var cachedUrl = require('qaq-core-util').cachedUrl;
    if(formatNme != '' && coIdn !=''){
        var params = {
            "db":connection,
            "format":formatNme,
            "coIdn":coIdn
        }
        let data = await coreUtil.mailFormat(params);
            data = data || {};
            var isData=Object.keys(data) || [];
            var isDatalen=isData.length;
            if(isDatalen > 0){
                if(data["status"] == 'SUCCESS'){
                    //console.log(data);
                    var subject = data["subject"];
                    var body = data["body"];
                    body = body.replace("~filename",filename);
                    body = body.replace("~body",message);

                    var from = '';
                    if(sal_source != 'NA')
                            from = 'From '+sal_source;

                    subject = subject.replace("~SUBJ",subjects);

                    var recipientlist = data["recipientlist"];
                    var to = recipientlist["to"];
                    var cc= recipientlist["cc"];
                    var bcc = recipientlist["bcc"];

                    var ccNew = ccId.concat(cc);
                    var bccNew = bccId.concat(bcc);
                    let toNew = emailIds.concat(to);
                    coreUtil.getCache("dbms_"+coIdn,cachedUrl).then(dbmsDtldata =>{
                        if(dbmsDtldata == null){
                               outJson["status"]="FAIL";
                               outJson["message"]="Fail to get DBMS Attribute";
                               callback(null,outJson);
                        }else{ 
                            dbmsDtldata = JSON.parse(dbmsDtldata);
                            var smtpuser = dbmsDtldata["smtpuser"];
                            var smtppassword = dbmsDtldata["smtppassword"];
                            var smtphost = dbmsDtldata["smtphost"];
                            var smtpport = dbmsDtldata["smtpport"];
                            var senderId = dbmsDtldata["senderid"];
                            let attachment = {};
                            attachment["path"] = "files/"+filename;
                          
                            var mailOptions = {
                                smtphost:smtphost,
                                smtpuser:smtpuser,
                                smtppassword:smtppassword,
                                smtpport:smtpport,
                                secure:true,
                                from: senderId, // sender address
                                cc:ccNew,
                                bcc:bccNew,
                                to: toNew, // list of receivers
                                subject: subject, // Subject line
                                html: body, // html body
                                attachments: attachment
                                };
                            //console.log(mailOptions);
                            coreUtil.sendMail(mailOptions).then(mailResult =>{
                                //console.log(mailResult);
                                if(mailResult.status == 'SUCCESS'){
                                    resultFinal["count"] = 1;
                                    outJson["result"]=resultFinal;
                                    outJson["status"]="SUCCESS";
                                    outJson["message"]="Mail Sent Successfully!";
                                    callback(null,outJson);
                                } else {
                                    outJson["result"]=resultFinal;
                                    outJson["status"]="FAIL";
                                    outJson["message"]="Mail Sent Failed!";
                                    callback(null,outJson);
                                } 
                            }) 
                        }
                    })
                }else{
                    callback(null,data);
                }
                }else{
                    outJson["result"]='';
                    outJson["status"]="FAIL";
                    outJson["message"]="Please Verify Format or Client Idn Parameter!";
                    callback(null,outJson);
                }
            
         //});   
    }else if(formatNme == ''){
         outJson["result"]='';
         outJson["status"]="FAIL";
         outJson["message"]="Please Verify Format Name Parameter";
         callback(null,outJson);
    }else if(coIdn == ''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Company Idn Parameter";
        callback(null,outJson);
   }
}

exports.getCountryFile = async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    var paramJson={};  
    var outJson={};
    let resultFinal = {};

    let filename =  'state';
    let fileExtension = 'csv';
    

    let list = [];
    let params = [];
    let fmt = {};
    let query = "select name,country_idn from states order by state_idn";
   
    //console.log(query);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
        if (error) {
            console.log(error);
            outJson["result"] = '';
            outJson["status"] = "FAIL";
            outJson["message"] = "gen_file_ary Fail To Execute Query!";
            callback(null, outJson);
        } else {
            let rowCount = result.rows.length;
            if (rowCount > 0) {
                var len = result.rows.length;
                //console.log("file Packet Len"+len);
                let resultView = [];
                resultView.push("name");
                resultView.push("country_idn");
                for (let i = 1; i < len; i++) {
                    let rows = result.rows[i];
                    let map ={};
                    map["name"]  = rows["name"] || '';
                    map["country_idn"]  = rows["country_idn"];
                    list.push(map);
                }

                filename = filename+'.csv';   
                let filePath = "files/"+filename;               
                const json2csvParser = new Json2csvParser({ resultView });
                const csv = json2csvParser.parse(list);
                //console.log(csv)
                fs.writeFile('files/'+filename, csv,async function(err) {
                if (err) {
                        console.log("error",err)
                        outJson["result"]=resultFinal;
                        outJson["status"]="FAIL";
                        outJson["message"]="CSV Download Fail";
                        callback(null,outJson);
                    }
                    resultFinal["filename"] = filename;
                    resultFinal["filePath"] = filePath;
                    outJson["result"]=resultFinal;
                    outJson["status"]="SUCCESS";
                    outJson["message"]="SUCCESS";
                    callback(null,outJson);
                });
            }
        }
    })          
}

exports.checkIncompleteTransaction =function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    let outJson = {};

    let intervalTime = req.body.intervalTime || '15';
    var poolsList = require('qaq-core-db').poolsList;
    var pool = poolsList[poolName] || '';
    if (pool != '') {
        coreDB.getTransPoolConnect(pool, async function (error, tpoolconn) {
            if (error) {
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "Fail To Get Conection!";
                callback(null, outJson);
            } else {
                let methodParamlocal = {};
                methodParamlocal["coIdn"] = coIdn;
                let formResult = await execGetFormDtl(methodParamlocal, tpoolconn);
                if(formResult.status == 'SUCCESS'){
                    let formList = formResult.result || [];

                    methodParamlocal = {};
                    methodParamlocal["coIdn"] = coIdn;
                    methodParamlocal["formList"] = formList;
                    methodParamlocal["intervalTime"] = intervalTime;
                    let logResult = await execGetAccessLogDtl(methodParamlocal, tpoolconn);
                    if(logResult.status == 'SUCCESS'){
                        let logList = logResult.result || [];
                        let resultView = logResult.resultView || [];
                        let resultViewDtl = logResult.resultViewDtl || {};
    
                        methodParamlocal={};
                        methodParamlocal["coIdn"]=coIdn;  
                        methodParamlocal["logList"]=logList;
                        methodParamlocal["formatNme"]="accesslogreq";
                        methodParamlocal["resultView"]=resultView;
                        methodParamlocal["resultViewDtl"]=resultViewDtl;
                        let logMailResult = await coreUtil.sendAccessLogMail(methodParamlocal, tpoolconn);
                        if(logMailResult.status == 'SUCCESS'){

                            coreDB.doTransRelease(tpoolconn);
                            outJson["status"] = "SUCCESS";
                            outJson["message"] = "Log Mail send successfully";
                            callback(null, outJson);
                        } else {
                            coreDB.doTransRelease(tpoolconn);
                            callback(null, logMailResult);
                        }
                    } else {
                        coreDB.doTransRelease(tpoolconn);
                        callback(null, logResult);
                    }
                } else {
                    coreDB.doTransRelease(tpoolconn);
                    callback(null, formResult);
                }  
            }
        })
    } else {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Fail To Get Conection!";
        callback(null, outJson);
    }
}

function execGetFormDtl(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getFormDtl(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function getFormDtl(tpoolconn, paramJson, callback) {
    var coIdn = paramJson.coIdn;
    let outJson = {};
    let formList = [];

    let params = [];
    let fmt = {};
    let query = "select form_nme from module_page where stt=1 and flg='T' ";

    //console.log(query);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
        if (error) {
            console.log(error);
            outJson["result"] = '';
            outJson["status"] = "FAIL";
            outJson["message"] = "getFormDtl Fail To Execute Query!";
            callback(null, outJson);
        } else {
            var len = result.rows.length;
            if (len > 0) {
                for(let i=0;i<len;i++){
                    let data = result.rows[i] || {};
                    let form_nme = data.form_nme || '';
                    formList.push(form_nme);
                }

                outJson["result"] = formList;
                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                callback(null, outJson);
            } else {
                outJson["status"] = "FAIL";
                outJson["message"] = "Form List data not found";
                callback(null, outJson);
            }
        }
    });
}

function execGetAccessLogDtl(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getAccessLogDtl(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function getAccessLogDtl(tpoolconn, paramJson, callback) {
    var coIdn = paramJson.coIdn;
    let formList = paramJson.formList || [];
    let intervalTime = parseInt(paramJson.intervalTime);
    let outJson = {};
    let logDtlList = [];

    if(formList.length > 0){
        let params = [];
        let fmt = {};
        let query = " select c.username,get_nme(c.nme_idn) buyer,a.access_log_idn,\n"+
                "a.log_idn,a.form_nme,a.request_method, \n"+
                "to_char(a.request_ts + interval'5.5 hours', 'dd-Mon HH24:mi:ss') dt \n"+ 
                "from appl_access_log a,appl_login_log b,appl_user c where  \n"+
                "a.log_idn = b.log_idn and b.user_idn=c.user_idn  \n"+
                "and c.co_idn=$1 and form_nme in ('" + formList.join("','") + "') \n"+
                //"and a.request_ts::date = current_date-10  \n"+
                "and a.request_ts >= current_timestamp - ('"+intervalTime+" minutes')::interval \n"+
                "and response_ts is null ";
        params.push(coIdn);
    
        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
            if (error) {
                console.log(error);
                outJson["result"] = '';
                outJson["status"] = "FAIL";
                outJson["message"] = "getAccessLogDtl Fail To Execute Query!";
                callback(null, outJson);
            } else {
                var len = result.rows.length;
                if (len > 0) {
                    for(let i=0;i<len;i++){
                        let data = result.rows[i] || {};
                        let map = {};
                        map["username"] = data.username || '';
                        map["buyer"] = data.buyer || '';
                        map["access_log_idn"] = data.access_log_idn || '';
                        map["log_idn"] = data.log_idn || '';
                        map["form_nme"] = data.form_nme || '';
                        map["request_method"] = data.request_method || '';
                        map["request_date"] = data.dt || '';
                        logDtlList.push(map);
                    }
                    let resultView = [];
                    resultView.push("username");
                    resultView.push("buyer");
                    resultView.push("access_log_idn");
                    resultView.push("log_idn");
                    resultView.push("form_nme");
                    resultView.push("request_method");
                    resultView.push("request_date");

                    let resultViewDtl = {};
                    resultViewDtl["username"] = "UserName";
                    resultViewDtl["buyer"] = "Buyer";
                    resultViewDtl["access_log_idn"] = "Access Log Idn";
                    resultViewDtl["log_idn"] = "Log Idn";
                    resultViewDtl["form_nme"] = "Form Name";
                    resultViewDtl["request_method"] = "Request Method";
                    resultViewDtl["request_date"] = "Request Date";

    
                    outJson["result"] = logDtlList;
                    outJson["resultView"] = resultView;
                    outJson["resultViewDtl"] = resultViewDtl;
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "SUCCESS";
                    callback(null, outJson);
                } else {
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Access Log data not found";
                    callback(null, outJson);
                }
            }
        });
    } else if(formList.length == 0){
        outJson["status"] = "FAIL";
        outJson["message"] = "Form list parameter can not be blank ";
        callback(null, outJson);
    }
}

exports.ezUnlock =async function(req,res,tpoolconn,redirectParam,callback) {
    let parameter = req.body.parameter || '';
    
    let methodParam = {};
    methodParam["password"] = parameter;
    let decryptResult = await coreUtil.execUnlock(methodParam);
    callback(null,decryptResult);
}

exports.getGIAStockDtl = async function(req,res,tpoolconn,redirectParam,callback) {
    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let log_idn = redirectParam.log_idn;
    let poolName = redirectParam.poolName;
    let prefix = redirectParam.prefix || '';
    var outJson={};

    let reportNoList = req.body.reportNoList || [];
    let reportNoListLen = reportNoList.length;
    if(reportNoListLen > 0){
        let resultView = [];
        resultView.push("shape_and_cutting_style");
        resultView.push("carat_weight");
        resultView.push("color_grade");
        resultView.push("clarity_grade");
        resultView.push("cut_grade");
        resultView.push("fluorescence");
        resultView.push("polish");
        resultView.push("symmetry");
        resultView.push("inscriptions");
        resultView.push("report_comments");
        resultView.push("clarity_characteristics");
        resultView.push("diamond_type");
        resultView.push("depth_pct");
        resultView.push("table_pct");
        resultView.push("crown_angle");
        resultView.push("crown_height");
        resultView.push("pavilion_angle");
        resultView.push("pavilion_depth");
        resultView.push("star_length");
        resultView.push("lower_half");
        resultView.push("girdle");
        resultView.push("culet");

        let attrDisplayDtl = {};
        for(let j=0;j<resultView.length;j++){
            let attr = resultView[j];
            let methodParams = {};
            methodParams["attr"] = attr;
            methodParams["coIdn"] = coIdn;
            let attrResult =await execGetAttrDetails(methodParams,tpoolconn);
            if(attrResult.status == 'SUCCESS'){
                let map = attrResult["result"] || {};
                attrDisplayDtl[attr+"_V"] = map["attr"] || '';
                attrDisplayDtl[attr+"_T"] = map["dta_typ"] || '';
            }
        }

        var cachedUrl = require('qaq-core-util').cachedUrl;
       
        let productAttributeDtl = await coreUtil.getCache(prefix+"productAttributeDtl_"+coIdn,cachedUrl);
        if(productAttributeDtl == null){
            outJson["status"]="FAIL";
            outJson["message"]="Fail to get Product Sub Attribute";
            callback(null,outJson);
        }else{
            productAttributeDtl = JSON.parse(productAttributeDtl);

            for(let i=0;i<reportNoListLen;i++){
                let reportNo = reportNoList[i];

                let methodParam = {};
                methodParam["coIdn"] = coIdn;
                methodParam["reportNo"] = reportNo;
                methodParam["productAttributeDtl"] = productAttributeDtl;
                methodParam["resultView"] = resultView;
                methodParam["attrDisplayDtl"] = attrDisplayDtl;
                let giaResult =await execGetGIAData(methodParam);
            }
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson); 
        }
    } else {
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify reportNoList Can not be blank!";
        callback(null, outJson);
    }
}


function execGetGIAData(methodParam) {
    return new Promise(function (resolve, reject) {
        getGIAData( methodParam,  function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

async function getGIAData(paramJson, callback){
    let reportNos = paramJson.reportNo || '';
    let coIdn = paramJson.coIdn;
    let productAttributeDtl = paramJson.productAttributeDtl;
    let resultView = paramJson.resultView || [];
    let attrDisplayDtl = paramJson.attrDisplayDtl || {};
    let outJson = {};
    let resultFinal = {};
    console.log(reportNos);

    let obj = {
        query: `
          {
          getReport(report_number: "`+reportNos+`") {
            report_date
            report_number
            report_type
            results {
              ... on DiamondGradingReportResults {
                shape_and_cutting_style
                carat_weight
                color_grade
                clarity_grade
                cut_grade
                fluorescence
                polish
                symmetry
                inscriptions
                report_comments
                clarity_characteristics
                diamond_type
                proportions {
                  depth_pct
                  table_pct
                  crown_angle
                  crown_height
                  pavilion_angle
                  pavilion_depth
                  star_length
                  lower_half
                  girdle
                  culet
                }
              }
            }
          }
        } 
        ` 
      };

    var options = {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'Authorization': '9dbd8aa4-0838-4e05-9538-24f07ede7632'
        },
        body: JSON.stringify(obj)
      };
      //console.log(options);
    const GRAPHQL_URL = 'https://api.reportresults.gia.edu/'
 
    const response = await fetch(GRAPHQL_URL, options)

    const responseBody = await response.json();
    //console.log(responseBody);
    let data = responseBody.data || {};
    let getReport = data.getReport || {};
    let reportData = getReport.results || {};
    console.log(reportData);
   
    let stockMap = {};
    for(let i=0;i<resultView.length;i++){
        let giaAttr = resultView[i];
        let attr = attrDisplayDtl[giaAttr+"_V"] || '';
        let data_typ = attrDisplayDtl[giaAttr+"_T"] || '';
        let attrVal = reportData[giaAttr] || '';
        if(attrVal == ''){
            let proportions = reportData["proportions"] || {};
            attrVal = proportions[giaAttr] || '';
        }

        if (data_typ == 'c' && attrVal != '' && attrVal != 0) {
            let optVals = productAttributeDtl[attr+"#O"] || []; 
            let prpSort = productAttributeDtl[attr + "#S"] || [];
            let prpVal = productAttributeDtl[attr + "#V"] || [];
            for(let m=0;m<optVals.length;m++){
                let element = optVals[m] || [];
                if(element.indexOf(attrVal) > -1){
                    attrVal = prpSort[m] || '';  
                } 
            }
        }
        stockMap[attr] = attrVal;       
    }

    console.log("stockMap",stockMap);
    outJson["status"] = "SUCCESS";
    outJson["message"] = "SUCCESS";
    callback(null, outJson); 
}

function execGetAttrDetails(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getAttrDetails(methodParam, tpoolconn, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getAttrDetails(methodParam, tpoolconn, callback) {
    let attr = methodParam.attr;
    let coIdn = methodParam.coIdn;
    let fmt = {};
    let params = [];
    let outJson = {};

    var sql = "select * from get_attr_nme_by_optval($1)";
    params.push(attr);

    //console.log(sql);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, sql, params, fmt, function (error, result) {
        if (error) {
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In getAttrDetails Method!";
            callback(null, outJson);
        } else {
            var len = result.rows.length;
            if (len > 0) {
                let rows = result.rows[0];
                let map = {};
                map["attr"] = rows.pattr;
                map["dta_typ"] = rows.pdtatyp;

                outJson["status"] = "SUCCESS";
                outJson["message"] = "SUCCESS";
                outJson["result"] = map;
                callback(null, outJson);
            } else {
                outJson["status"] = "FAIL";
                outJson["message"] = "Sorry No Result Found";
                callback(null, outJson);
            }
        }
    });
}

function execUpdateStockMData(methodParam, connection) {
    return new Promise(function (resolve, reject) {
        updateStockMData(methodParam, connection, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function updateStockMData(methodParam, connection, callback) {
    var logUsr = methodParam.logUsr || '';
    var reportNo = methodParam.reportNo || '';
    let coIdn = methodParam.coIdn;
    let attrMap = methodParam.attrMap || {};
    let stock_idn = methodParam.stock_idn || [];
    let outJson = {};
    let updateQ = "";
    let params = [];

    updateQ = "update stock_m set  attr = attr || concat('"+JSON.stringify(attrMap)+"')::jsonb  " +
        ", modified_ts = current_timestamp, modified_by = $1 " +
        "where stock_idn in ("+stock_idn.join(",")+") and stt = 1 and co_idn = $2 ";


    params.push(logUsr);
    params.push(coIdn);

    //console.log(updateQ);
    //console.log(params);
    coreDB.executeTransSql(connection, updateQ, params, {}, function (error, result) {
        if (error) {
            coreDB.doTransRollBack(connection);
            outJson["status"] = "FAIL";
            outJson["message"] = "Error In update stock_m Method!" + error.message;
            console.log(outJson);
            callback(null, outJson);
        } else {
            //coreDB.doTransCommit(connection);
            outJson["status"] = "SUCCESS";
            outJson["message"] = "SUCCESS";
            callback(null, outJson);
        }
    })
}

exports.approvePackets = async function (req, res, oracleconnection, redirectParam, callback) {
    let pktIdnList = req.body.pktIdnList || [];
    let typ = req.body.typ || 'WH';
    let status = req.body.status || 'IS';
    let nme_idn = req.body.nme_idn || '';
    let resultFinal = {};
    let outJson = {}; 
    let methodParam = {};   
    let pktIdnListLen = pktIdnList.length;

    if(pktIdnListLen > 0 && nme_idn != ''){
        if(status == 'CF' || status == 'IS'){
            methodParam = {};
            methodParam["nme_idn"] = nme_idn;
            let nmeDtlResult = await execGetNmeDtl(methodParam,oracleconnection);
            if(nmeDtlResult.status == "SUCCESS"){
                let rel_idn = nmeDtlResult.result || '';
    
                methodParam = {};
                methodParam["rel_idn"] = rel_idn;
                methodParam["pktIdnList"] = pktIdnList;
                let pktDtlResult = await execInsertPktDataToGt(methodParam,oracleconnection);
                if(pktDtlResult.status == "SUCCESS" && pktDtlResult.message == "SUCCESS"){
                    let avlPacketList = pktDtlResult["result"] || [];
                    let avlPacketListLen = avlPacketList.length;
                   
                    methodParam = {};
                    methodParam["rel_idn"] = rel_idn;
                    methodParam["nme_idn"] = nme_idn;
                    methodParam["status"] = status;
                    methodParam["inv_typ"] = typ;
                    let invResult = await execInsertWebInvMasData(methodParam,oracleconnection);
                    if(invResult.status == 'SUCCESS'){
                        if(parseInt(pktIdnListLen) == parseInt(avlPacketListLen)){
                            resultFinal["invId"] = invResult["result"];
                            resultFinal["validPackets"] = avlPacketList;
                            outJson["result"] = resultFinal;
                            outJson["status"] = "SUCCESS";
                            outJson["message"] = "SUCCESS";//alreadyApvPacketList.toString()+" This packets are already approved.";
                            callback(null, outJson); 
                        }else {
                            //console.log("pktIdnList",pktIdnList);
                            //console.log("avlPacketList",avlPacketList);
                            let alreadyApvPacketList = pktIdnList.filter(function(val) {
                                    return avlPacketList.indexOf(val) == -1;
                              });
                            
                            resultFinal["invId"] = invResult["result"] || 1234;
                            resultFinal["invalidPackets"] = alreadyApvPacketList;
                            resultFinal["validPackets"] = avlPacketList;
                            outJson["result"] = resultFinal;
                            outJson["status"] = "SUCCESS";
                            outJson["message"] = "SUCCESS";//alreadyApvPacketList.toString()+" This packets are already approved.";
                            callback(null, outJson); 
                        }
                    } else {
                        callback(null, invResult); 
                    } 
                } else {
                    callback(null,pktDtlResult);
                }
            } else {
                callback(null, nmeDtlResult);
            }
        } else if(status == 'RT' ){
            for(let i=0;i<pktIdnList.length;i++){
                let pktIdn = pktIdnList[i];

                methodParam = {};
                methodParam["nme_idn"] = nme_idn;
                methodParam["pktIdn"] = pktIdn;
                let invoiceDtlResult = await execGetInvoiceDtl(methodParam,oracleconnection);
                if(invoiceDtlResult.status == "SUCCESS"){
                    let inv_id = invoiceDtlResult.result || [];
                    let mstkIdn = invoiceDtlResult.mstkIdn || '';
                    let memo_id = invoiceDtlResult.memo_id || [];
  
                    methodParam = {};
                    methodParam["inv_id"] = inv_id;
                    methodParam["pktIdn"] = pktIdn;
                    methodParam["status"] = status;
                    methodParam["mstkIdn"] = mstkIdn;
                    methodParam["memo_id"] = memo_id;
                    invoiceDtlResult = await execReturnInvoiceDtl(methodParam,oracleconnection);
                }
            }
            outJson["result"] = resultFinal;
            outJson["status"] = "SUCCESS";
            outJson["message"] = "Packets Return successfully";
            callback(null, outJson);        
        }  
    } else if(pktIdnListLen == 0){
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "pktIdnList can not be blank";
        callback(null, outJson);
    }  else if(nme_idn == ''){
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "nme_idn can not be blank";
        callback(null, outJson);
    }
}

function execGetNmeDtl(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getNmeDtl(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getNmeDtl(connection,paramJson,callback) {
    let nme_idn = paramJson.nme_idn || '';
    let outJson = {};
    
    let oraclefmt = {outFormat: oracledb.OBJECT};
    let oracleparams = {};
    var query=" select nmerel_idn from nme_rel_v where nme_idn = :nme_idn and dflt_yn='Y' ";
    
    oracleparams= {nme_idn};

    //console.log(query);
    //console.log(oracleparams);
    coreDB.executeSql(connection,query,oracleparams,oraclefmt,function(error,result){
        if(error){
            console.log(error);
            outJson["status"]="FAIL";
            outJson["message"]="Fail To Execute Query!";
            callback(null,outJson);   
        }else{
            var len = result.rows.length;
            if (len > 0) {
                let rel_idn = result.rows[0].NMEREL_IDN;
                //console.log(rel_idn);
                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                outJson["result"]=rel_idn;
                callback(null,outJson);
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="Customer Idn is incorrect or there is no defult term exit. plz try again.";
                callback(null,outJson);
            }
        }
    }) 
}

function execInsertPktDataToGt(methodParam,oracleconnection){
    return new Promise(function(resolve,reject) {
        insertPktDataToGt(methodParam,oracleconnection,function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
     });
    });
}

function insertPktDataToGt(redirectParam,oracleconnection,callback){
    let pktIdnList = redirectParam.pktIdnList || [];
    let rel_idn = redirectParam.rel_idn || '';
    let outJson = {};
    let resultFinal = {};

    if(rel_idn != ''){
        var oracleparams = {};
        var oraclefmt = {autoCommit:true};
        var query = "Delete from gt_srch_rslt";
        //console.log(query);
        coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
            if(error){
                outJson["status"]="FAIL";
                outJson["message"]="Fail To Execute Query of delete gt_srch_rslt!";
                callback(null,outJson);   
            }else{
                var rowNum = result.rowsAffected;
                console.log("delete",rowNum)
                query =  "select  b.vnm \n"+
                    "from mstk b where b.stt in ('MKAV','BRAV','LB_PRI','MKIS','MKWH','MKEI') and ( vnm in ('" + pktIdnList.join("','") + "') or tfl3 in ('" + pktIdnList.join("','") + "') )  ";
                oracleparams = {};
                oraclefmt = {outFormat: oracledb.OBJECT};
                //console.log(query);
                //console.log(oracleparams);
                coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
                    if(error){
                        console.log(error);
                        outJson["status"]="FAIL";
                        outJson["message"]="Fail To Execute Query of select mstk!";
                        callback(null,outJson);   
                    }else{
                        let len = result.rows.length;
                        let avlPacketList = [];
                        //console.log(len);
                        for(let i=0;i<len;i++){
                            let obj = result.rows[i] || {};
                            let vnm = obj.VNM || '';
                            //console.log(vnm);
                            avlPacketList.push(vnm);
                        }
                        query = "Insert into gt_srch_rslt ( rln_idn, srch_id, pkt_ty, stk_idn, vnm,rmk, qty, cts, pkt_dte, stt,prte, cmp, rap_rte, cert_lab, cert_no, flg, sk1, quot, rap_dis ) \n"+
                            "select  :rel_idn,1 srch_id, b.pkt_ty, b.idn, b.vnm, b.tfl3, decode(b.pkt_ty, 'NR', b.qty, b.qty - nvl(qty_iss,0)) qty,decode(b.pkt_ty, 'NR', b.cts, b.cts - nvl(cts_iss, 0)) cts,\n"+
                            " b.dte, b.stt,b.fcpr, nvl(upr, cmp) rte, b.rap_rte, b.cert_lab, b.cert_no,"+
                            " 'I' flg, sk1, nvl(upr,cmp) , decode(b.rap_rte, 1, null, trunc((nvl(upr,cmp)/rap_rte*100)-100, 2)) rap_dis "+ 
                            "from mstk b where b.stt in ('MKAV','BRAV','LB_PRI','MKIS','MKWH','MKEI') and ( vnm in ('" + pktIdnList.join("','") + "') or tfl3 in ('" + pktIdnList.join("','") + "') )  ";
                        oracleparams = {};
                        oracleparams= {rel_idn};
                        oraclefmt = {autoCommit:true};
                        //console.log(query);
                        //console.log(oracleparams);
                        coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
                            if(error){
                                console.log(error);
                                outJson["status"]="FAIL";
                                outJson["message"]="Fail To Execute Query of insert gt_srch_rslt!";
                                callback(null,outJson);   
                            }else{
                                var rowNum = result.rowsAffected;
                                console.log("Insert",rowNum)
                                if(rowNum > 0) {
                                    oracleparams = {rel_idn};
                                    query = "call pkgmkt.cal_quot( pRlnId=> :rel_idn)";
                                    //console.log(query);
                                    coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
                                        if(error){
                                            outJson["status"]="FAIL";
                                            outJson["message"]="Fail To Execute Query of Cal_Quot!";
                                            callback(null,outJson);   
                                        }else{
                                            outJson["result"] = avlPacketList;
                                            outJson["status"]="SUCCESS";
                                            outJson["message"]="SUCCESS";
                                            callback(null,outJson);
                                        }
                                    })
                                } else {
                                    resultFinal["invalidPackets"] = pktIdnList;
                                    outJson["result"] = resultFinal;
                                    outJson["status"]="SUCCESS";
                                    outJson["message"]="All given packets are approved already";
                                    callback(null,outJson);  
                                }
                            }
                        })
                    }
                })
            }
        })
    } else if(rel_idn == ''){
        outJson["status"] = "FAIL";
        outJson["message"] = "rel_idn can not be blank";
        callback(null, outJson);
    }
}

function execInsertWebInvMasData(methodParam,oracleconnection){
    return new Promise(function(resolve,reject) {
        insertWebInvMasData(methodParam,oracleconnection,function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
     });
    });
}

function insertWebInvMasData(methodParam,oracleconnection,callback){
    let nme_idn = methodParam.nme_idn;
    let rel_idn = methodParam.rel_idn;
    let inv_typ = methodParam.inv_typ;
    let status = methodParam.status;
    let outJson = {};
   
    let oraclefmt = {outFormat: oracledb.OBJECT};
    let oracleparams = {};
    var query=" select seq_inv_id.nextval inv_id from dual ";

    console.log(query);
    //console.log(oracleparams);
    coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
        if(error){
            outJson["status"]="FAIL";
            outJson["message"]="Fail To Execute Query!";
            callback(null,outJson);   
        }else{
            var len = result.rows.length;
            if (len > 0) {
                let inv_id = result.rows[0].INV_ID;
                oraclefmt = {};
                let insertQ="insert into web_minv(dte, inv_id, log_id, usr_id, mcust_idn,  rel_idn, exh_rte, inv_typ, tm_pct, rln) "+
                    "values(sysdate,:inv_id,0,0,:nme_idn,:rel_idn,1,:inv_typ,1,'USD')";

                oracleparams = {};
                oracleparams={inv_id,nme_idn,rel_idn,inv_typ};
                 //console.log(insertQ);
                 //console.log(oracleparams);
                coreDB.executeSql(oracleconnection,insertQ,oracleparams,oraclefmt,function(error,result){
                    if(error){
                        coreDB.doRollBack(oracleconnection);
                        outJson["status"]="FAIL";
                        outJson["message"]="Error In insert web_minv Method!"+error.message;
                        callback(null,outJson);
                    }else{
                        //console.log("mktg_salmas",result)                       
                        coreDB.doCommit(oracleconnection);
                        var rowCount = result.rowsAffected;
                        if(rowCount!=0){     
                            insertQ="insert into web_inv_dtl (inv_id, mstk_idn, cert, qty, cts, rte, quot, rap_dis, dte, stt) \n"+
                                " select  :inv_id , stk_idn, cert_lab, qty, cts , cmp ,Quot, decode(rap_rte, 1, null, trunc((((Quot*100)/1)/rap_rte) - 100,2)) r_dis, sysdate, :status \n" +
                                " from gt_srch_rslt where flg = 'I' ";
        
                            oracleparams = {};
                            oracleparams={inv_id,status};
                             //console.log(insertQ);
                             //console.log(oracleparams);
                            coreDB.executeSql(oracleconnection,insertQ,oracleparams,oraclefmt,function(error,result){
                                if(error){
                                    coreDB.doRollBack(oracleconnection);
                                    outJson["status"]="FAIL";
                                    outJson["message"]="Error In insert web_inv_dtl Method!"+error.message;
                                    callback(null,outJson);
                                }else{
                                    //console.log("mktg_salmas",result)                       
                                    coreDB.doCommit(oracleconnection);
                                    var rowCount = result.rowsAffected;
                                    if(rowCount!=0){       
                                        oracleparams = {inv_id};
                                        query = "call  memo_pkg.makeFromInv(:inv_id)";
                                        //console.log(query);
                                        coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
                                            if(error){
                                                outJson["status"]="FAIL";
                                                outJson["message"]="Fail To Execute Query of makeFromInv!";
                                                callback(null,outJson);   
                                            }else{
                                                outJson["result"]=inv_id;
                                                outJson["status"]="SUCCESS";
                                                outJson["message"]="SUCCESS";
                                                callback(null,outJson);
                                            }
                                        })
                                    }else{
                                        coreDB.doRollBack(oracleconnection);
                                        outJson["status"]="FAIL";
                                        outJson["message"]="web_inv_dtl Insertion Failed!";
                                        callback(null,outJson);
                                    } 
                                }
                            }) 
                        }else{
                            coreDB.doRollBack(oracleconnection);
                            outJson["status"]="FAIL";
                            outJson["message"]="web_minv Insertion Failed!";
                            callback(null,outJson);
                        } 
                    }
                }) 
            }
        }
    })          
}

function execGetInvoiceDtl(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getInvoiceDtl(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

function getInvoiceDtl(connection,paramJson,callback) {
    let nme_idn = paramJson.nme_idn || '';
    let pktIdn = paramJson.pktIdn || '';
    let outJson = {};
    
    let oraclefmt = {outFormat: oracledb.OBJECT};
    let oracleparams = {};
    //var query="  select b.inv_id,c.rel_idn,c.mcust_idn,b.alc_memo,a.idn mstkIdn from mstk a,web_inv_dtl b,web_minv c \n"+ 
    //    "where a.idn=b.mstk_idn and b.inv_id = c.inv_id and  b.stt in ('IS','CF')  and ( vnm in ('" + pktIdnList.join("','") + "') or tfl3 in ('" + pktIdnList.join("','") + "') ) \n"+
    //    "and c.mcust_idn = :nme_idn ";
    //let query = "with alcId as ( \n"+
    //    "select max(b.inv_id) mxId \n"+
    //    "from mstk a,web_inv_dtl b,web_minv c  \n"+
    //    "where a.idn=b.mstk_idn and b.inv_id = c.inv_id and  b.stt in ('IS','CF') \n"+
    //    "and ( vnm = '"+pktIdn+"' or tfl3 = '"+pktIdn+"' ) \n"+
    ///    "and c.mcust_idn = :nme_idn and alc_memo is not null) \n"+
    //    "select inv_id, alc_memo, mstk_idn \n"+
    //    "from web_inv_dtl a, alcId \n"+
    //    "where a.inv_id = alcId.mxId ";
    let query = "select max(b.inv_id) mxId, a.idn mstk_idn,b.alc_memo \n"+
        "from mstk a,web_inv_dtl b,web_minv c  \n"+
        "where a.idn=b.mstk_idn and b.inv_id = c.inv_id and  b.stt in ('IS','CF') \n"+
        "and vnm = '"+pktIdn+"' and c.mcust_idn = :nme_idn and alc_memo is not null \n"+
        "group by a.idn,alc_memo ";
    
    oracleparams= {nme_idn};

    //console.log(query);
    //console.log(oracleparams);
    coreDB.executeSql(connection,query,oracleparams,oraclefmt,function(error,result){
        if(error){
            outJson["status"]="FAIL";
            outJson["message"]="Fail To Execute Query!";
            callback(null,outJson);   
        }else{
            var len = result.rows.length;
            if (len > 0) {
                let inv_id = result.rows[0].MXID;
                let memo_id = result.rows[0].ALC_MEMO || '';
                let mstkIdn = result.rows[0].MSTK_IDN;

                outJson["status"]="SUCCESS";
                outJson["message"]="SUCCESS";
                outJson["result"]=inv_id;
                outJson["memo_id"]=memo_id;
                outJson["mstkIdn"]=mstkIdn;
                callback(null,outJson);
            }else{
                outJson["status"]="FAIL";
                outJson["message"]="Invoice Details not found.";
                callback(null,outJson);
            }
        }
    }) 
}

function execReturnInvoiceDtl(methodParam,oracleconnection){
    return new Promise(function(resolve,reject) {
        returnInvoiceDtl(methodParam,oracleconnection,function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
     });
    });
}

function returnInvoiceDtl(methodParam,oracleconnection,callback){
    let inv_id = methodParam.inv_id || '';
    let pktIdn = methodParam.pktIdn || '';
    let status = methodParam.status;
    let memo_id = methodParam.memo_id || '';
    let mstkIdn = methodParam.mstkIdn || '';
    let outJson = {};
   
    let oraclefmt = {};
    let sql="update web_inv_dtl set stt = :status where \n"+
        " inv_id =:inv_id and mstk_idn =:mstkIdn";

    let oracleparams = {};
    oracleparams={status,inv_id,mstkIdn};
        //console.log(sql);
        //console.log(oracleparams);
    coreDB.executeSql(oracleconnection,sql,oracleparams,oraclefmt,function(error,result){
        if(error){
            coreDB.doRollBack(oracleconnection);
            outJson["status"]="FAIL";
            outJson["message"]="Error In update web_inv_dtl Method!"+error.message;
            callback(null,outJson);
        }else{                      
            coreDB.doCommit(oracleconnection);
            var rowCount = result.rowsAffected;
            if(rowCount!=0){ 
                sql="update jandtl set stt = :status where \n"+
                " idn =:memo_id and mstk_idn =:mstkIdn ";

            oracleparams = {};
            oracleparams={status,memo_id,mstkIdn};
                //console.log(sql);
                //console.log(oracleparams);
            coreDB.executeSql(oracleconnection,sql,oracleparams,oraclefmt,function(error,result){
                if(error){
                    coreDB.doRollBack(oracleconnection);
                    outJson["status"]="FAIL";
                    outJson["message"]="Error In update jandtl Method!"+error.message;
                    callback(null,outJson);
                }else{                      
                    coreDB.doCommit(oracleconnection);
                    var rowCount = result.rowsAffected;
                    if(rowCount!=0){     
                        sql="update mstk set stt='MKAV' where  ( vnm =:pktIdn or tfl3 =:pktIdn )  ";

                        oracleparams = {};
                        oracleparams ={pktIdn};
                        //console.log(sql);
                        //console.log(oracleparams);
                        coreDB.executeSql(oracleconnection,sql,oracleparams,oraclefmt,function(error,result){
                            if(error){
                                coreDB.doRollBack(oracleconnection);
                                outJson["status"]="FAIL";
                                outJson["message"]="Error In update mstk Method!"+error.message;
                                callback(null,outJson);
                            }else{                    
                                coreDB.doCommit(oracleconnection);
                                var rowCount = result.rowsAffected;
                                if(rowCount!=0){  
                                    oracleparams = {memo_id};
                                    query = "call  jan_qty(:memo_id)";
                                    //console.log(query);
                                    coreDB.executeSql(oracleconnection,query,oracleparams,oraclefmt,function(error,result){
                                        if(error){
                                            outJson["status"]="FAIL";
                                            outJson["message"]="Fail To Execute Query of makeFromInv!";
                                            callback(null,outJson);   
                                        }else{
                                            outJson["status"]="SUCCESS";
                                            outJson["message"]="Packets return successfully";
                                            callback(null,outJson);     
                                        }
                                    })       
                                }else{
                                    coreDB.doRollBack(oracleconnection);
                                    outJson["status"]="FAIL";
                                    outJson["message"]="Mstk Updation Failed!";
                                    callback(null,outJson);
                                } 
                            }
                        }) 
                    } else{
                        coreDB.doRollBack(oracleconnection);
                        outJson["status"]="FAIL";
                        outJson["message"]="JanDtl Updation Failed!";
                        callback(null,outJson);
                    } 
                }
                })
            }else{
                coreDB.doRollBack(oracleconnection);
                outJson["status"]="FAIL";
                outJson["message"]="web_inv_dtl Updation Failed!";
                callback(null,outJson);
            } 
        }
    }) 
                
}
