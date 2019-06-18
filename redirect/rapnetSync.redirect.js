const coreUtil = require('qaq-core-util');
const coreDB = require('qaq-core-db');
var rapnetSyncController = require('../controller/rapnetSync.controller');
var async = require("async");

exports.load = function(req, res,callback) {
    var outJson = {};
    var method = req.headers['method'] || '';
    var clientkey = req.headers['clientkey'] || '';
    var modulekey = req.headers['modulekey'] || '';
    var loginid = req.headers['loginid'] || '';
    var source = req.headers['source'] || '';
    var formNme= req.body.formNme || '';
    var bodyparam=req.body;
    bodyparam["source"]=source;
    var params = {};
    let accessParams={};
    if(method !='' && clientkey !='' && modulekey !='' && loginid !=''){
        params = {};
        params["clientKey"]=clientkey;
        params["moduleKey"]=modulekey; 
        var cachedUrl = require('qaq-core-util').cachedUrl;
        coreUtil.tokenValidation(params,cachedUrl).then(tokenValidationdata =>{
            tokenValidationdata = tokenValidationdata || {};
            var tokenValidationKeys=Object.keys(tokenValidationdata) || [];
            var tokenValidationKeyslen=tokenValidationKeys.length;
            if(tokenValidationKeyslen > 0){
                if(tokenValidationdata["status"]== 'SUCCESS'){
                    var ds=tokenValidationdata["ds"] || '';
                    var poolName=tokenValidationdata["pool"] || 'TPOOL';
                    if(ds!=''){
                        var poolsList= require('qaq-core-db').poolsList;
                        var pool = poolsList[poolName] || '';
                        if(pool!=''){
                            coreDB.getTransPoolConnect(pool,async function(error,connection){
                                if(error){
                                    outJson["result"]='';
                                    outJson["status"]="FAIL";
                                    outJson["message"]="Fail To Get Conection!";
                                    res.send(outJson);
                                }else{
                                    params = {};
                                    params["loginid"]=loginid;
                                    params["db"]=connection; 
                                    coreUtil.isLogin(params).then(isLogindata=>{
                                        isLogindata = isLogindata || {};
                                        var isLogindataKeys=Object.keys(isLogindata) || [];
                                        var isLogindataKeyslen=isLogindataKeys.length;
                                        if(isLogindataKeyslen > 0){
                                            if(isLogindata["status"]== 'SUCCESS'){
                                                accessParams={};
                                                accessParams["log_idn"] = loginid;
                                                accessParams["pg_name"] = formNme;
                                                accessParams["req_method"] = method;
                                                accessParams["req_data"] = bodyparam;
                                                accessParams["db"] = connection;
                                                coreUtil.updateAccessLog(accessParams).then(insertAccessLogResult=>{
                                                    let accessLogId = insertAccessLogResult["result"].LOGID;
                                                    if(insertAccessLogResult["status"]== 'SUCCESS'){
                                                        var pvtConfigure = isLogindata["category"] || '';
                                                        var logUsr = isLogindata["logUser"] || '';
                                                        
                                                        var applIdn = isLogindata["applIdn"] || '';
                                                        var coIdn = isLogindata["coIdn"] || '';
                                                        var userIdn = isLogindata["userIdn"] || '';
                                                        let nmeIdn = isLogindata["nmeIdn"] || '';
                                                        let dept = isLogindata["dept"] || '';
                                                        let grpNmeIdn = isLogindata["grpNmeIdn"] || '';
                                                        let groupEmpList = isLogindata["groupEmpList"]||[];
                                                        let userType = isLogindata["userType"]||'';
                                                        if(logUsr !=''  && coIdn!='' && userIdn!=''){
                                                            if(typeof rapnetSyncController[''+method] === 'function'){
                                                                let methodParam={};
                                                                methodParam["userIdn"]=userIdn;
                                                                methodParam["coIdn"]=coIdn;
                                                                methodParam["pvtConfigure"]=pvtConfigure;
                                                                methodParam["logUsr"]=logUsr;
                                                                methodParam["applIdn"]=applIdn;
                                                                methodParam["loginid"]=loginid;
                                                                methodParam["nmeIdn"]=nmeIdn;
                                                                methodParam["dept"]=dept;
                                                                methodParam["grpNmeIdn"]=grpNmeIdn;
                                                                methodParam["groupEmpList"]=groupEmpList;
                                                                methodParam["userType"]=userType;

                                                                methodParam["source"]=source;
                                                                rapnetSyncController[''+method](req,res,connection,methodParam,function(error,result){
                                                                    let res_data={};
                                                                    res_data["status"]=result.status || '';
                                                                    res_data["message"]=result.message || '';
                                                                    accessParams["res_data"] = res_data;
                                                                    accessParams["access_log_idn"]=accessLogId;
                                                                // console.log(accessParams);
                                                                    coreUtil.updateAccessLog(accessParams).then(updAccessLogResult=>{
                                                                        coreDB.doTransCommit(connection);
                                                                        coreDB.doTransRelease(connection);
                                                                        res.send(result);
                                                                    });
                                                                });
                                                            }else{
                                                                outJson["result"]='';
                                                                outJson["status"]="FAIL";
                                                                outJson["message"]="Please Verify Method Name Parameter!";
                                                                accessParams["res_data"] = outJson;
                                                                accessParams["access_log_idn"]=accessLogId;
                                                                coreUtil.updateAccessLog(accessParams).then(updAccessLogResult=>{
                                                                    coreDB.doTransRelease(connection);
                                                                    res.send(outJson);
                                                                });
                                                            }
                                                        }else if(logUsr ==''){
                                                            outJson["result"]='';
                                                            outJson["status"]="FAIL";
                                                            outJson["message"]="Please Verify Login User From isLogin() Can not be blank!";
                                                            accessParams["res_data"] = outJson;
                                                            accessParams["access_log_idn"]=accessLogId;
                                                            coreUtil.updateAccessLog(accessParams).then(updAccessLogResult=>{
                                                                coreDB.doTransRelease(connection);
                                                                res.send(outJson);
                                                            });
                                                        }else if(coIdn ==''){
                                                            outJson["result"]='';
                                                            outJson["status"]="FAIL";
                                                            outJson["message"]="Please Verify Login coIdn From isLogin() Can not be blank!";
                                                            accessParams["res_data"] = outJson;
                                                            accessParams["access_log_idn"]=accessLogId;
                                                            coreUtil.updateAccessLog(accessParams).then(updAccessLogResult=>{
                                                                coreDB.doTransRelease(connection);
                                                                res.send(outJson);
                                                            });
                                                        }else if(userIdn ==''){
                                                            outJson["result"]='';
                                                            outJson["status"]="FAIL";
                                                            outJson["message"]="Please Verify Login userIdn From isLogin() Can not be blank!";
                                                            accessParams["res_data"] = outJson;
                                                            accessParams["access_log_idn"]=accessLogId;
                                                            coreUtil.updateAccessLog(accessParams).then(updAccessLogResult=>{
                                                                coreDB.doTransRelease(connection);
                                                                res.send(outJson);
                                                            })
                                                        }
                                                    }else{
                                                        outJson["result"]='';
                                                        outJson["status"]="FAIL";
                                                        outJson["message"]="Please Verify Login Access!";
                                                        coreDB.doTransRelease(connection);
                                                        res.send(outJson);
                                                    }
                                                });
                                            }else{
                                                coreDB.doTransRelease(connection);
                                                res.send(isLogindata);
                                            }
                                        }else{
                                            coreDB.doTransRelease(connection);
                                            outJson["result"]='';
                                            outJson["status"]="FAIL";
                                            outJson["message"]="Please Verify isLogin Can not be blank!";
                                            res.send(outJson);
                                        }
                                    });
                                }
                            });
                        }else{
                            outJson["result"]='';
                            outJson["status"]="FAIL";
                            outJson["message"]="Fail To Get Conection!";
                            res.send(outJson);
                        }
                    }else{
                        outJson["result"]='';
                        outJson["status"]="FAIL";
                        outJson["message"]="Please Verify DS Can not be blank!";
                        res.send(outJson);
                    }
                }else{
                    res.send(tokenValidationdata);
                }
            }else{
                outJson["result"]='';
                outJson["status"]="FAIL";
                outJson["message"]="Please Verify Module Key/client Key Parameter!";
                res.send(outJson);
            }
        });
    }else if(clientkey ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Client Key Can not be blank!";
    res.send(outJson);
    }else if(modulekey ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Module Key Can not be blank!";
    res.send(outJson);
    }else if(method ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Method Name Can not be blank!!";
    res.send(outJson);
    }else if(loginid ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Log Idn Can not be blank!!";
    res.send(outJson);
    }
}
