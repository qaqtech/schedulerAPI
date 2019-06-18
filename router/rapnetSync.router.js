var express = require('express');
var router = express.Router();
var redirect = require('../redirect/rapnetSync.redirect');

router.post('/load', redirect.load);

module.exports = router;