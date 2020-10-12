var express = require('express');
var router = express.Router();
var redirect = require('../redirect/rapnetSync.redirect');
var schedule = require('../redirect/schedule.redirect');

router.post('/load', redirect.load);
router.post('/scheduleApi', schedule.scheduleApi);
router.post('/scheduleLoad', schedule.scheduleLoad);
router.post('/webservice', schedule.load);

module.exports = router;