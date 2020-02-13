var express = require('express');
var router = express.Router();
var redirect = require('../redirect/rapnetSync.redirect');
var schedule = require('../redirect/schedule.redirect');

router.post('/load', redirect.load);
router.post('/schedule', schedule.schedule);
router.post('/scheduleApi', schedule.scheduleApi);

module.exports = router;