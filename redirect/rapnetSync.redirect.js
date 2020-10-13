const coreUtil = require('qaq-core-util');
const coreDB = require('qaq-core-db');
var rapnetSyncController = require('../controller/rapnetSync.controller');

exports.load = function(req, res) {
    var outJson = {};
    var method = req.headers['method'] || '';
    var moduleKey = req.headers['modulekey'] || '';
    var clientKey = req.headers['clientkey'] || '';
    var source = req.headers['source'] || 'api';
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
                                            if(typeof rapnetSyncController[''+method] === 'function'){
                                                let methodParam={};
                                                methodParam["clientKey"]=clientKey;
                                                methodParam["coIdn"]=coIdn;
                                                methodParam["source"]=source;
                                                rapnetSyncController[''+method](req, res ,connection,methodParam,function(error,result){
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
                                    })
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
