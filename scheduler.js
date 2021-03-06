const express = require('express');
const bodyParser = require('body-parser');
const compression = require('compression');
const coreDB = require('qaq-core-db');
const util = require('qaq-core-util');
const cors = require('cors');

const rapsync = require('./router/rapnetSync.router');


const app = express();
const hostname = '0.0.0.0';
const port = 8140;

util.cachedUrl="52.74.209.117:80";


app.use(compression());
app.use(cors());

// parse requests of content-type - application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({  limit: '1000mb', extended: true }))

// parse requests of content-type - application/json
app.use(bodyParser.json({ limit: '1000mb',extended: true }));

app.use("/",rapsync);


app.listen(port, hostname, () => {
    util.getCache("clientModuleKeys",util.cachedUrl).then(data => {
		//console.log(data);
		if(data!=''){
         var result = JSON.parse(data);
		 var moduleDtl = result['SbPYRdIImGIhlRR6sQjJWN8AYfOUDOTp']||'';
		 var poolNme = moduleDtl.pool||'TPOOL';
		 let poolList=[];
		 var databaseList = ["","GR_"];

		 for(var i=0 ; i < databaseList.length; i++){
			var db = databaseList[i]; 
			var pool=db+poolNme;
			poolList.push(pool);
		}
		 poolList.push("MFGPOOL");
		 poolList.push("KGFAPOOL");
		 poolList.push("CDORCL");
		 coreDB.initializePoolsList(poolList).then(poolsList => {
             coreDB.poolsList=poolsList;
               console.log(`transactionPools Created `);
                     console.log(`Server running at http://${hostname}:${port}/`);
            });
		}
	});
});