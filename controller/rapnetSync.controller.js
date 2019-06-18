const coreDB = require('qaq-core-db');
var oracledb = require("oracledb");
const coreUtil = require('qaq-core-util');
var request = require('request');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
var dateFormat = require('dateformat');

exports.rapnetPacketDelete =async function (req, res, connection, redirectParam, callback) {
    var formNme = req.body.formNme || '';

    var coIdn = redirectParam.coIdn;
    var applIdn = redirectParam.applIdn;
    let source = redirectParam.source || req.body.source;
    let outJson = {};
    let methodParam = {};
    var resultFinal = {};
    var now = new Date();
    var dte=dateFormat(now, "ddmmmyyyydh.MM.ss");

    if (formNme != '') {
        methodParam = {};
        methodParam["coIdn"] = coIdn;
        let tokenResult = await execGetToken(methodParam);
        if(tokenResult.status == 'SUCCESS'){

            methodParam = {};
            methodParam["coIdn"] = coIdn;
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
                    
                const json2csvParser = new Json2csvParser({ resultView });
                const csv = json2csvParser.parse(packetDtlList);
                let filename = 'deletePackets_'+dte;
                fs.writeFile('files/'+filename+'.csv', csv, function(err) {
                    if (err) {
                    console.log("error",err)
                    outJson["result"]=resultFinal;
                    outJson["status"]="FAIL";
                    outJson["message"]="CSV Download Fail";
                    callback(null,outJson);
                    }
                    let filePath = 'files/'+filename+'.csv';

                    let uploadPath = "http://technet.rapaport.com/HTTP/Upload/Upload.aspx?Method=file&ReplaceAll=false&ticket=" + token;

          

                    request.post({
                        url: uploadPath,
                        headers: { 'Content-Type': 'multipart/form-data'},
                        formData: {
                            file: fs.createReadStream('files/deletePackets_17Jun2019173.04.30.csv'),
                            filetype: 'csv',
                            filename: 'deletePackets_17Jun2019173.04.30',
                            title: 'deletePackets',
                        },
                        }, function(error, response, body) {
                            console.log("response"+response.statusCode); 
                            if (error) {
                                console.error('upload failed:', error);
                                }
                                console.log('Server responded with:', body);
                        });

                    outJson["result"]=filename+'.csv';
                    outJson["status"]="SUCCESS";
                    outJson["message"]="File Uploaded Successfully!";
                    callback(null,outJson);    
                });                                                         
            } else {
                callback(null,fileArrayResult);
            } 
        } else {
            callback(null, tokenResult);
        }  
    } else if (formNme == '') {
        outJson["result"] = resultFinal;
        outJson["status"] = "FAIL";
        outJson["message"] = "Please Verify formNme Can not be blank!";
        callback(null, outJson);
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
    let coIdn = paramJson.coIdn;
    let outJson = {};
    let resultFinal = {};
  
    var headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  
    var options = {
        url: 'https://technet.rapaport.com/HTTP/Authenticate.aspx',
        method: 'POST',
        headers: headers,
        form: {Username:'66444',Password:'Key86Values'}
    };
    request(options,function (error, response, body) { 
        if (!error && response.statusCode == 200) {
            console.log("response"+response.statusCode); 
            console.log("Token"+body);
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
    let outJson = {};
    let list = [];

    let params = [];
    let fmt = {};
    let query = "select gen_file_ary_delete($1, 'rapnet_ind', 15) del_ary";
    params.push(coIdn);
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