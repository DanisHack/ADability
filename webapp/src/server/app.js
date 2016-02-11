'use strict';
var express = require('express');
var path = require('path');
var logger = require('morgan');
var bodyParser = require('body-parser');
var compress = require('compression');
var handlebars = require('express-handlebars');
var chronoshift_1 = require('chronoshift');
// Init chronoshift
if (!chronoshift_1.WallTime.rules) {
    var tzData = require("chronoshift/lib/walltime/walltime-data.js");
    chronoshift_1.WallTime.init(tzData.rules, tzData.zones);
}
var config_1 = require('./config');
var plywoodRoutes = require('./routes/plywood/plywood');
var app = express();
app.disable('x-powered-by');
// view engine setup
var viewsDir = path.join(__dirname, '../../src/views');
app.engine('.hbs', handlebars({
    defaultLayout: 'main',
    extname: '.hbs',
    layoutsDir: path.join(viewsDir, 'layouts'),
    partialsDir: path.join(viewsDir, 'partials')
}));
app.set('views', viewsDir);
app.set('view engine', '.hbs');
app.use(compress());
app.use(logger('dev'));
app.use(express.static(path.join(__dirname, '../../build/public')));
app.use(express.static(path.join(__dirname, '../../assets')));
app.use(bodyParser.json());
app.get('/', function (req, res, next) {
    config_1.DATA_SOURCE_MANAGER.getQueryableDataSources().then(function (dataSources) {
        if (dataSources.length) {
            var config = {
                version: config_1.VERSION,
                dataSources: dataSources.map(function (ds) { return ds.toClientDataSource(); }),
            };
            if (config_1.HIDE_GITHUB_ICON)
                config.hideGitHubIcon = config_1.HIDE_GITHUB_ICON;
            if (config_1.HEADER_BACKGROUND)
                config.headerBackground = config_1.HEADER_BACKGROUND;
            res.render('pivot', {
                version: config_1.VERSION,
                config: JSON.stringify(config),
                title: 'Pivot'
            });
        }
        else {
            res.render('no-data-sources', {
                version: config_1.VERSION,
                title: 'No Data Sources'
            });
        }
    }).done();
});
app.use('/plywood', plywoodRoutes);
app.get('/health', function (req, res, next) {
    res.send("Okay");
});

// Catch 404 and redirect to /
app.use(function (req, res, next) {
    res.redirect('/');
});
// error handlers
// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function (err, req, res, next) {
        res.status(err['status'] || 500);
        res.render('error', {
            message: err.message,
            error: err,
            version: config_1.VERSION,
            title: 'Error'
        });
    });
}
// production error handler
// no stacktraces leaked to user
app.use(function (err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {},
        version: config_1.VERSION,
        title: 'Error'
    });
});
module.exports = app;
