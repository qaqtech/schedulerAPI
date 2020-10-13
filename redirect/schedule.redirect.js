const coreUtil = require('qaq-core-util');
var scheduleController = require('../controller/schedule.controller');
const coreDB = require('qaq-core-db');

exports.scheduleApi = function(req, res) {
    var outJson = {};
    var method = req.headers['method'] || '';
    var moduleKey = req.headers['modulekey'] || '';
    var clientKey = req.headers['clientkey'] || '';
    var source = req.headers['source'] || 'api';
    var log_idn = req.headers['log_idn'] || '';
    var params = {};  
    if(method !='' && moduleKey !='' && clientKey !='' && source != ''){
        let inputData = req.body || {};
        params = {};
        params["inputData"]=inputData;
        coreUtil.inputValidation(params).then(inputValidationData =>{
            inputValidationData = inputValidationData || {};
            if(inputValidationData.status == 'SUCCESS'){
                var cachedUrl = require('qaq-core-util').cachedUrl;
                params = {};     
                params["clientKey"]=clientKey;
                params["moduleKey"]=moduleKey; 
                coreUtil.tokenValidation(params,cachedUrl).then(tokenValidationdata =>{
                tokenValidationdata = tokenValidationdata || {};
                var tokenValidationKeys=Object.keys(tokenValidationdata) || [];
                var tokenValidationKeyslen=tokenValidationKeys.length;
                if(tokenValidationKeyslen > 0){
                if(tokenValidationdata["status"]== 'SUCCESS'){
                        var poolName=tokenValidationdata["pool"] || 'TPOOL';
                        var coIdn = tokenValidationdata["coIdn"] || '';
                        let prefix = "";
                        if(poolName.indexOf("GR_")>-1)
                            prefix = "GR_";
                        if(poolName != ''){
                            var poolsList= require('qaq-core-db').poolsList;
                            poolName = poolName.trim();
                            var pool = poolsList[poolName] || '';
                            if(pool !=''){
                                coreDB.getTransPoolConnect(pool, function(error,connection){
                                    if(error){
                                        console.log(error);
                                        outJson["result"]='';
                                        outJson["status"]="FAIL";
                                        outJson["message"]="Fail To Get Conection!";
                                        res.send(outJson);
                                    }else{
                                        if(typeof scheduleController[''+method] === 'function'){
                                            let methodParam={};
                                            methodParam["clientKey"]=clientKey;
                                            methodParam["coIdn"]=coIdn;
                                            methodParam["source"]=source;
                                            methodParam["poolName"] =poolName;
                                            methodParam["log_idn"]=log_idn;
                                            methodParam["prefix"] = prefix;
                                            scheduleController[''+method](req, res ,connection,methodParam,function(error,result){
                                                coreDB.doTransRelease(connection);
                                                res.send(result);
                                            });
                                        }else{
                                            outJson["result"]='';
                                            outJson["status"]="FAIL";
                                            outJson["message"]="Please Verify Method Name Parameter!";
                                            coreDB.doTransRelease(connection);
                                            res.send(outJson);
                                        }
                                    }
                                });
                            }else{
                                outJson["result"]='';
                                outJson["status"]="FAIL";
                                outJson["message"]="Please Verify Pool from PoolList can not be blank!";
                                res.send(outJson);
                            }
                        }else{
                            outJson["result"]='';
                            outJson["status"]="FAIL";
                            outJson["message"]="Please Verify Pool Name can not be blank!";
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
                })
            } else {
                res.send(inputValidationData);
            }
        })
   }else if(moduleKey ==''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Module Key can not be blank!";
        res.send(outJson);
   }else if(method ==''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Method Name can not be blank!";
        res.send(outJson);
   }else if(clientKey ==''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Client Key can not be blank!";
        res.send(outJson);
   }else if(source ==''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Source can not be blank!";
        res.send(outJson);
   }
}

exports.scheduleLoad = function(req, res) {
    var outJson = {};
    var method = req.headers['method'] || '';
    var moduleKey = req.headers['modulekey'] || '';
    var source = req.headers['source'] || 'api';
    let params = {};
    if(method !='' && moduleKey !='' && source != ''){
        let inputData = req.body || {};
        params = {};
        params["inputData"]=inputData;
        coreUtil.inputValidation(params).then(inputValidationData =>{
            inputValidationData = inputValidationData || {};
            if(inputValidationData.status == 'SUCCESS'){
                var cachedUrl = require('qaq-core-util').cachedUrl;    
                coreUtil.getCache("ModuleKeys",cachedUrl).then(moduleDtldata=>{         
                    if(moduleDtldata != null)
                        moduleDtldata = JSON.parse(moduleDtldata);

                    var poolName= moduleDtldata[moduleKey] || 'TPOOL';
                    //console.log("poolName",poolName);
                    let prefix = "";
                    if(poolName.indexOf("GR_")>-1)
                        prefix = "GR_";
                    var poolsList= require('qaq-core-db').poolsList;
                    var pool = poolsList[poolName] || '';
                    if(pool!=''){
                        coreDB.getTransPoolConnect(pool, function(error,connection){
                            if(error){
                                console.log(error);
                                outJson["result"]='';
                                outJson["status"]="FAIL";
                                outJson["message"]="Fail To Get Conection!";
                                res.send(outJson);
                            }else{
                                if(typeof scheduleController[''+method] === 'function'){
                                    let methodParam={};
                                    methodParam["source"]=source;
                                    methodParam["poolName"] =poolName;
                                    methodParam["prefix"] = prefix;
                                    scheduleController[''+method](req, res ,connection,methodParam,function(error,result){
                                        coreDB.doTransRelease(connection);
                                        res.send(result);
                                    });
                                }else{
                                    outJson["result"]='';
                                    outJson["status"]="FAIL";
                                    outJson["message"]="Please Verify Method Name Parameter!";
                                    coreDB.doTransRelease(connection);
                                    res.send(outJson);
                                }
                            }
                        });
                    }else{
                        outJson["result"]='';
                        outJson["status"]="FAIL";
                        outJson["message"]="Please Verify Pool from PoolList can not be blank!";
                        res.send(outJson);
                    }
                }) 
            } else {
                res.send(inputValidationData);
            }
        }) 
   }else if(moduleKey ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Module Key can not be blank!";
    res.send(outJson);
   }else if(method ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Method Name can not be blank!";
    res.send(outJson);
   }else if(source ==''){
    outJson["result"]='';
    outJson["status"]="FAIL";
    outJson["message"]="Please Verify Source can not be blank!";
    res.send(outJson);
   }
}

exports.load = function(req, res,callback) {
    var outJson = {};
    var method = req.headers['method'] || 'approvePackets';
    var ds = req.headers['ds'] || '';
    var params = {};
    let accessParams={};
    if(method !='' && ds !=''){
        let inputData = req.body || {};
        params = {};
        params["inputData"]=inputData;
        coreUtil.inputValidation(params).then(inputValidationData =>{
            inputValidationData = inputValidationData || {};
            if(inputValidationData.status == 'SUCCESS'){
                let poolName = ds;//+"POOL";
                poolName = poolName.trim();   
                console.log(poolName);            
                coreDB.getPoolConnect(poolName,async function(error,oracleconnection){
                    if(error){                                   
                        outJson["status"]="FAIL";
                        outJson["message"]="Fail To Get Oracle Conection!";
                        callback(null,outJson);   
                    }else{
                        if(typeof scheduleController[''+method] === 'function'){ 
                            let methodParam = {};                                                             
                            scheduleController[''+method](req, res ,oracleconnection,methodParam,function(error,result){
                                coreDB.doCommit(oracleconnection);
                                coreDB.doRelease(oracleconnection);
                                res.send(result);
                            });
                        }else{
                            outJson["result"]='';
                            outJson["status"]="FAIL";
                            outJson["message"]="Please Verify Method Name Parameter!";
                            coreDB.doCommit(oracleconnection);
                            coreDB.doRelease(oracleconnection);
                            res.send(outJson);
                        } 
                    }   
                })  
            } else {
                res.send(inputValidationData);
            }
        })              
    }else if(method ==''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Method Name Can not be blank!!";
        res.send(outJson);
    }else if(ds ==''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="Please Verify Ds Can not be blank!!";
        res.send(outJson);
    }
}
