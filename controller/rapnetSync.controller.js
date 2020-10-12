const coreDB = require('qaq-core-db');
var oracledb = require("oracledb");
const coreUtil = require('qaq-core-util');
var request = require('request');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
var dateFormat = require('dateformat');
let async = require("async");
const soapRequest = require('easy-soap-request');

exports.rapnetPacketDelete =async function (req, res, connection, redirectParam, callback) {
    var formNme = req.body.formNme || '';
    var timePeriod = req.body.timePeriod || '15';

    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let outJson = {};
    let methodParam = {};
    var resultFinal = {};
    var now = new Date();
    var dte=dateFormat(now, "ddmmmyyyydh.MM.ss");

    if (formNme != '') {
        methodParam = {};
        methodParam["coIdn"] = coIdn;
        methodParam["timePeriod"] = timePeriod;
        let fileArrayResult = await execGetFileDeletePackets(methodParam,connection);
        if(fileArrayResult.status == 'SUCCESS'){
            let resultView = fileArrayResult["resultView"] || [];
            let packetDetails = fileArrayResult["packetDetails"] || [];
            let resultViewlen = resultView.length;

            let packetDtlList = [];
            let pktIdnList = [];
            // console.log(packetDetails.length);
            for(let i=0;i<packetDetails.length;i++){
                let pktdtl = packetDetails[i] || [];
                let packetDtl = {};
                for(let j=0;j<resultViewlen;j++){
                    let attr = resultView[j];
                    let attrVal = pktdtl[j] || '';
                    packetDtl[attr] = attrVal;
                    if(attr == 'Stock #')
                        pktIdnList.push(attrVal);
                }
                packetDtlList.push(packetDtl);
            }

            methodParam = {};
            methodParam["coIdn"] = coIdn;
            let fileOptionResult = await execGetFileOptionsDtl(methodParam,connection);
            if(fileOptionResult.status == 'SUCCESS'){
                let fileOptionDtl = fileOptionResult.result || {};
                let filename = 'Rapnet_Delete_'+dte; //fileOptionDtl["filename"];
                let fileExtension = fileOptionDtl["fileExtension"];
                let usernamelist = fileOptionDtl["username"] || [];
                usernamelist = JSON.parse(usernamelist);
                let passwordlist = fileOptionDtl["password"] || [];
                passwordlist = JSON.parse(passwordlist);

                //console.log("packetDtlList length ",packetDtlList.length);
                //console.log("usernamelist",usernamelist);
                if(pktIdnList.length > 0){
                    //console.log("usernamelist",usernamelist.length);
                    let tokenList = [];
                    for (let i = 0; i < usernamelist.length; i++) {
                        let username = usernamelist[i];
                        let password = passwordlist[i];
                        let methodParamLocal = {};
                        methodParamLocal["username"] = username;
                        methodParamLocal["password"] = password;
                        let tokenResult = await execGetToken(methodParamLocal);
                        if(tokenResult.status == 'SUCCESS'){
                            let token = tokenResult.result || '';
                            if(token != '')
                                tokenList.push(token);
                        }
                    }

                    for (let i = 0; i < pktIdnList.length; i++) {
                        let pktIdn = pktIdnList[i];
                        for (let j = 0; j < tokenList.length; j++) {
                            let ticket = tokenList[j];
                            let methodParamLocal = {};
                            methodParamLocal["pktIdn"] = pktIdn;
                            methodParamLocal["ticket"] = ticket;
                            let pktResult = execDeletePackets(methodParamLocal);

                        }
                    }



                    methodParam = {};
                    methodParam["resultView"]=resultView;
                    methodParam["coIdn"] = coIdn;
                    methodParam["empidn"]=coIdn;
                    methodParam["formatNme"] = 'rapnet_delete';
                    methodParam["pktDetails"]=packetDtlList;
                    methodParam["buyerYN"]="No";
                    methodParam["byridn"]=coIdn;
                    methodParam["packetDisplayCnt"]=10;
                    methodParam["usernamelist"] = usernamelist;
                    let mailResult = await coreUtil.sendRapnetDeleteMail(methodParam,connection);
                    console.log("mailResult",mailResult);
                    outJson["result"] = resultFinal;
                    outJson["status"] = "SUCCESS";
                    outJson["message"] = "SUCCESS";
                    callback(null, outJson);
                            
                } else {
                    outJson["result"] = resultFinal;
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Packets not found for deletion";
                    callback(null, outJson);
                }  
            } else {
                callback(null,fileOptionResult);
            }                                                        
        } else {
            callback(null,fileArrayResult);
        } 
    } else if (formNme == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify formNme Can not be blank!";
        callback(null, outJson);
    }
}

function execDeletePackets(methodParam){
    return new Promise(function(resolve,reject) {
        deletePackets(methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

async function deletePackets(paramJson){
    let pktIdn = paramJson.pktIdn || '';
    let ticket = paramJson.ticket || '';
    let outJson = {};
    let resultFinal = {};
    //console.log("paramJson",paramJson);
  
    if(pktIdn != '' && ticket != ''){
        const url = 'https://technet.rapaport.com/webservices/Upload/DiamondManager.asmx';

        const deleteheaders = {
            'user-agent': 'sampleTest',
            'Content-Type': 'text/xml;charset=UTF-8',
            'soapAction': 'http://technet.rapaport.com/DeleteLots',
            };
    
        const deletexml = '<?xml version="1.0" encoding="utf-8"?> '+
                    '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"> '+
                    '<soap:Header> '+
                    '<AuthenticationTicketHeader xmlns="http://technet.rapaport.com/"> '+
                    '<Ticket>'+ticket+'</Ticket> '+
                    '</AuthenticationTicketHeader> '+
                    '</soap:Header> '+
                    '<soap:Body> '+
                    '<DeleteLots xmlns="http://technet.rapaport.com/"> '+
                    '<Parameters> '+
                    '<ByField>StockNum</ByField> '+
                    '<FieldValueList>'+pktIdn+'</FieldValueList> '+
                    '</Parameters> '+
                    '</DeleteLots> '+
                    '</soap:Body> '+
                    '</soap:Envelope>'; 

        (async () => {
            const { response } = await soapRequest(url, deleteheaders, deletexml, 7000); // Optional timeout parameter(milliseconds)
            const { body, statusCode } = response;
            console.log("deletebody",body);
            console.log("deleteStatus",statusCode);

        })();
    }
}

function execGetToken(methodParam){
    return new Promise(function(resolve,reject) {
        getToken(methodParam, function (error, result) {
            if(error){  
                reject(error);
            }
            resolve(result);
        });
    });
}

function getToken(paramJson,callback){
    let username = paramJson.username || '';
    let password = paramJson.password || '';
    let outJson = {};
    let resultFinal = {};
    //console.log("username",username);
    //console.log("password",password);
    if(username != '' && password != ''){
        const url = 'https://technet.rapaport.com/webservices/Upload/DiamondManager.asmx';
        const headers = {
            'user-agent': 'sampleTest',
            'Content-Type': 'text/xml;charset=UTF-8',
            'soapAction': 'http://technet.rapaport.com/Login',
            };
        const xml = '<?xml version="1.0" encoding="utf-8"?> '+
            '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"> '+
            '<soap:Body> '+
            '<Login xmlns="http://technet.rapaport.com/"> '+
            '<Username>'+username+'</Username> '+
            '<Password>'+password+'</Password> '+
            '</Login> '+
            '</soap:Body> '+
            '</soap:Envelope>';
            
        (async () => {
            const { response } = await soapRequest(url, headers, xml, 7000); // Optional timeout parameter(milliseconds)
            const { body, statusCode } = response;
            //console.log(body);
            console.log("loginStatus",statusCode);
            if(statusCode == 200){
                let arr = body.split("<Ticket>");
                let arr2 = arr[1] || '';
                let arr3 = arr2.split("</Ticket>"); 
                let ticket = arr3[0];
                //console.log("ticket",arr3[0]);

                outJson["result"]=ticket;
                outJson["message"]="SUCCESS";
                outJson["status"]="SUCCESS";
                callback(null,outJson);  
            }else{
                outJson["message"]=body;
                outJson["status"]="FAIL";
                callback(null,outJson);   
            }
        })();
    } else if(username == ''){
            outJson["result"]='';
            outJson["status"]="FAIL";
            outJson["message"]="username can not be blank";
            callback(null,outJson);
    } else if(password == ''){
        outJson["result"]='';
        outJson["status"]="FAIL";
        outJson["message"]="password can not be blank";
        callback(null,outJson);
    }
}

function execGetFileDeletePackets(methodParam, tpoolconn) {
    return new Promise(function (resolve, reject) {
        getFileDeletePackets(tpoolconn, methodParam, function (error, result) {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });

}

function getFileDeletePackets(tpoolconn, paramJson, callback) {
    var coIdn = paramJson.coIdn || '';
    let timePeriod = paramJson.timePeriod;
    let outJson = {};
    let list = [];

    let params = [];
    let fmt = {};
    let query = "select gen_file_ary_delete($1, 'rapnet_ind', $2) del_ary";
    params.push(coIdn);
    params.push(parseInt(timePeriod));
    //console.log(query);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
        if (error) {
            outJson["result"] = '';
            outJson["status"] = "FAIL";
            outJson["message"] = "gen_file_ary_delete Fail To Execute Query!";
            callback(null, outJson);
        } else {
            let rowCount = result.rowCount;
            if (rowCount > 0) {
                var len = result.rows.length;
                let resultView = result.rows[0].del_ary;
                for (let i = 1; i < len; i++) {
                    let rows = result.rows[i];
                    let obj  = rows["del_ary"];
                    list.push(obj);
                }
                outJson["resultView"] = resultView;
                outJson["packetDetails"] = list;
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
    var coIdn = paramJson.coIdn;
    let outJson = {};
    let map = {};

    let params = [];
    let fmt = {};
    let query = "select addl_attr->> 'filename' filename, "+
        "addl_attr->> 'fileExtension' fileExtension, "+
        "addl_attr->> 'username' username, "+	
        "addl_attr->> 'password' passwords, "+
        "file_idn , key_mapping,nme "+			  
        "from file_options  where co_idn=$1 and stt=1 and nme = 'rapnet_ind' ";

    params.push(coIdn);
    //console.log(query);
    //console.log(params);
    coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
        if (error) {
            outJson["result"] = '';
            outJson["status"] = "FAIL";
            outJson["message"] = "getFileOptionsDtl Fail To Execute Query!";
            callback(null, outJson);
        } else {
            var len = result.rows.length;
            if (len > 0) {
                map["filename"] = result.rows[0].filename;
                map["fileExtension"] = result.rows[0].fileextension;
                map["username"] = result.rows[0].username;
                map["password"] = result.rows[0].passwords;
                map["key_mapping"] = result.rows[0].key_mapping;
                map["file_idn"] = result.rows[0].file_idn;
                map["nme"] = result.rows[0].nme;

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
    })
}

exports.rapnetReplaceAll =async function (req, res, connection, redirectParam, callback) {
    var formNme = req.body.formNme || '';

    var coIdn = redirectParam.coIdn;
    let source = redirectParam.source || req.body.source;
    let outJson = {};
    let methodParam = {};
    var resultFinal = {};
    var now = new Date();
    var dte=dateFormat(now, "ddmmmyyyydh.MM.ss");

    if (formNme != '') {
        methodParam = {};
        methodParam["coIdn"] = coIdn;
        let fileOptionResult = await execGetFileOptionsDtl(methodParam,connection);
        if(fileOptionResult.status == 'SUCCESS'){
            let fileOptionDtl = fileOptionResult.result || {};
            let filename = 'Rapnet_ReplaceAll_'+dte; //fileOptionDtl["filename"];
            let fileExtension = fileOptionDtl["fileExtension"];
            let fileMap = fileOptionDtl["key_mapping"];
            let file_idn = fileOptionDtl["file_idn"];
            let usernamelist = fileOptionDtl["username"] || [];
            usernamelist = JSON.parse(usernamelist);
            let passwordlist = fileOptionDtl["password"] || [];
            passwordlist = JSON.parse(passwordlist);
            let nme = fileOptionDtl["nme"] || '';
            let filePath  = 'files/'+filename+'.csv';

            methodParam = {};
            methodParam["coIdn"] = coIdn;
            methodParam["nme"] = nme;
            let fileArrayResult = await execGenFileProcedure(methodParam,connection);
            if(fileArrayResult.status == 'SUCCESS'){
                let resultView = fileArrayResult["resultView"] || [];
                let packetDetails = fileArrayResult["packetDetails"] || [];
                let resultViewlen = resultView.length;

                let packetDtlList = [];
                // console.log(packetDetails.length);
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
            
                console.log("packetDtlList length ",packetDtlList.length);
                if(packetDtlList.length > 0){
                    if(fileExtension == 'csv'){
                        const json2csvParser = new Json2csvParser({ resultView });
                        const csv = json2csvParser.parse(packetDtlList);
                        fs.writeFile(filePath, csv,async function(err) {
                            if (err) {
                            console.log("error",err)
                            outJson["result"]=resultFinal;
                            outJson["status"]="FAIL";
                            outJson["message"]="CSV Download Fail";
                            callback(null,outJson);
                            } else {
                                    //console.log("usernamelist",usernamelist.length);
                                    let tokenList = [];
                                    for (let i = 0; i < usernamelist.length; i++) {
                                        let username = usernamelist[i];
                                        let password = passwordlist[i];
                                        let methodParamLocal = {};
                                        methodParamLocal["username"] = username;
                                        methodParamLocal["password"] = password;
                                        methodParamLocal["service_url"] = "https://technet.rapaport.com/HTTP/Authenticate.aspx";
                                        let tokenResult = await execGetAuthenticate(methodParamLocal);
                                        if(tokenResult.status == 'SUCCESS'){
                                            let token = tokenResult.result || '';
                                            if(token != '')
                                                tokenList.push(token);
                                        }
                                    }
                

                                    for (let j = 0; j < tokenList.length; j++) {
                                        let ticket = tokenList[j];
                                        let methodParamLocal = {};
                                        methodParamLocal["filePath"] = filePath;
                                        methodParamLocal["filename"] = filename+".csv";
                                        methodParamLocal["service_url"] = "http://technet.rapaport.com/HTTP/Upload/Upload.aspx?Method=file&ReplaceAll=false&ticket="+ticket;
                                        let pktResult =await execGetUploadFile(methodParamLocal);
            
                                    }

                                    methodParam = {};
                                    methodParam["resultView"]=resultView;
                                    methodParam["coIdn"] = coIdn;
                                    methodParam["empidn"]=coIdn;
                                    methodParam["formatNme"] = 'rapnet_delete';
                                    methodParam["pktDetails"]=packetDtlList;
                                    methodParam["buyerYN"]="No";
                                    methodParam["byridn"]=coIdn;
                                    methodParam["packetDisplayCnt"]=10;
                                    methodParam["usernamelist"] = usernamelist;
                                    //let mailResult = await coreUtil.sendRapnetDeleteMail(methodParam,connection);
                                    //console.log("mailResult",mailResult);
                                    outJson["result"] = resultFinal;
                                    outJson["status"] = "SUCCESS";
                                    outJson["message"] = "SUCCESS";
                                    callback(null, outJson);
                            }
                        });
                    }
                } else {
                    outJson["result"] = resultFinal;
                    outJson["status"] = "FAIL";
                    outJson["message"] = "Packets not found for deletion";
                    callback(null, outJson);
                }  
            } else {
                callback(null,fileArrayResult);
            }                                                        
        } else {
            callback(null,fileOptionResult);
        } 
    } else if (formNme == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify formNme Can not be blank!";
        callback(null, outJson);
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
    var nme = paramJson.nme || '';
    var coIdn = paramJson.coIdn || '';
    let outJson = {};
    let list = [];

    if (nme != '') {
        let params = [];
        let fmt = {};
        let query = "select gen_file_ary_update($1,$2) filearry";
        params.push(coIdn);
        params.push(nme);
        //console.log(query);
        //console.log(params);
        coreDB.executeTransSql(tpoolconn, query, params, fmt, function (error, result) {
            if (error) {
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
                    for (let i = 1; i < len; i++) {
                        let rows = result.rows[i];
                        let obj  = rows["filearry"];
                        list.push(obj);
                    }
                    outJson["resultView"] = resultView;
                    outJson["packetDetails"] = list;
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
    } else if (nme == '') {
        outJson["result"] = '';
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify File Name Parameter";
        callback(null, outJson);
    }
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
        url: service_url,
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
            outJson["result"] = body;
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
        'ContentType':'multipart/form-data'
    }

    var options = {
        url: service_url,
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
        console.log(body);
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
