const coreDB = require('qaq-core-db');
var oracledb = require("oracledb");
const coreUtil = require('qaq-core-util');
var request = require('request');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
var dateFormat = require('dateformat');
let async = require("async");

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
                let filePath  = 'files/'+filename+'.csv';
                console.log("packetDtlList length ",packetDtlList.length);
                //console.log("usernamelist",usernamelist);
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
                                for (let i = 0; i < usernamelist.length; i++) {
                                    let username = usernamelist[i];
                                    let password = passwordlist[i];
                                    let methodParamLocal = {};
                                    methodParamLocal["username"] = username;
                                    methodParamLocal["password"] = password;
                                    methodParamLocal["filePath"] = filePath;
                                    methodParamLocal["filename"] = filename;
                                    methodParamLocal["fileExtension"] = fileExtension;
                                    //console.log(methodParamLocal);
                                    deleteFileUpload(methodParamLocal);
                                }

                                methodParam = {};
                                methodParam["resultView"]=resultView;
                                methodParam["coIdn"] = coIdn;
                                methodParam["empidn"]="1";
                                methodParam["formatNme"] = 'rapnet_delete';
                                methodParam["pktDetails"]=packetDtlList;
                                methodParam["buyerYN"]="No";
                                methodParam["byridn"]="1";
                                methodParam["packetDisplayCnt"]=10;
                                methodParam["usernamelist"] = usernamelist;
                                let mailResult = await coreUtil.sendRapnetDeleteMail(methodParam,connection);
                                console.log("mailResult",mailResult);
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

async function deleteFileUpload(paramJson){
    let filePath = paramJson.filePath || '';
    let username = paramJson.username || '';
    let password = paramJson.password || '';
    let filename = paramJson.filename || '';
    let fileExtension = paramJson.fileExtension || '';
    let outJson = {};
    let resultFinal = {};
    console.log("paramJson",paramJson);
  
    if(filePath != '' && filename != '' && fileExtension != ''){
        let methodParam = {};
        methodParam["username"] = username;
        methodParam["password"] = password;
        let tokenResult = await execGetToken(methodParam);
        if(tokenResult.status == 'SUCCESS'){  
            let token = tokenResult.result;
    
            let uploadPath = "http://technet.rapaport.com/HTTP/Upload/Upload.aspx?Method=file&ReplaceAll=false&ticket=" + token;
            //console.log("uploadPath",uploadPath);
            var options = {
                url: uploadPath,
                headers: { 'Content-Type': 'multipart/form-data'},
                formData: {
                    file: fs.createReadStream(filePath),
                    filetype: fileExtension,
                    filename: filename,
                    title: 'deletePackets',
                }
            };
            //console.log("filePath",filePath)
            request.post(options, function(error, response, body) {
                console.log("uploadFileResponse"+response.statusCode); 
                console.log("error"+error); 
                if (error) {
                    console.error('upload failed:', error);
                }
                console.log('Server responded with:', body);
            })
        } 
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
        var headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }
    
        var options = {
            url: 'https://technet.rapaport.com/HTTP/Authenticate.aspx',
            method: 'POST',
            headers: headers,
            form: {Username:username,Password:password}
        };
        request(options,function (error, response, body) { 
            if (!error && response.statusCode == 200) {
                //console.log("response"+response.statusCode); 
                //console.log("Token"+body);
                let token = body;

                outJson["result"]=token;
                outJson["message"]="SUCCESS";
                outJson["status"]="SUCCESS";
                callback(null,outJson);  
            }else{
                console.log(error);
                outJson["message"]=error;
                outJson["status"]="FAIL";
                callback(null,outJson);   
            }
        });   
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
        "addl_attr->> 'password' passwords "+			  
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