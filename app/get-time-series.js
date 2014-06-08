'use strict';

var q = require('q'),
    getConfig = require('./get-config'),
    OAuth = require('oauth');

function getTimeSeries(app, user) {

    var config = getConfig(app),
        oauth = new OAuth.OAuth(
            'https://api.fitbit.com/oauth/request_token',
            'https://api.fitbit.com/oauth/access_token',
            config.fitbitClientKey,
            config.fitbitClientSecret,
            '1.0',
            null,
            'HMAC-SHA1'
        );

    return q.ninvoke(
        oauth,
        'post',
        'http://api.fitbit.com/1/user/-/activities/distance/date/2013-12-01/7d.json',
        user.token,
        user.tokenSecret,
        null,
        null
    ).then(function(data) {
        return data['activities-distance'];
    });
}

module.exports = getTimeSeries;