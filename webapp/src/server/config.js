'use strict';
var path = require('path');
var nopt = require('nopt');
var index_1 = require('../common/models/index');
var index_2 = require('./utils/index');
function errorExit(message) {
    console.error(message);
    process.exit(1);
}
var packageObj = null;
try {
    packageObj = index_2.loadFileSync(path.join(__dirname, '../../package.json'), 'json');
}
catch (e) {
    errorExit("Could not read package.json: " + e.message);
}
exports.VERSION = packageObj.version;
function printUsage() {
    console.log("\nUsage: pivot [options]\n\nPossible usage:\n\n  pivot --example wiki\n  pivot --druid your.broker.host:8082\n\n      --help                   Print this help message\n      --version                Display the version number\n  -v, --verbose                Display the DB queries that are being made\n  -p, --port                   The port pivot will run on\n      --example                Start pivot with some example data (overrides all other options)\n  -c, --config                 The configuration YAML files to use\n\n      --print-config           Prints out the auto generated config\n      --with-comments          Adds comments when printing the auto generated config\n      --data-sources-only      Only print the data sources in the auto generated config\n\n  -f, --file                   Start pivot on top of this file based data source (must be JSON, CSV, or TSV)\n\n  -d, --druid                  The Druid broker node to connect to\n      --introspection-strategy Druid introspection strategy\n          Possible values:\n          * segment-metadata-fallback - (default) use the segmentMetadata and fallback to GET route\n          * segment-metadata-only     - only use the segmentMetadata query\n          * datasource-get            - only use GET /druid/v2/datasources/DATASOURCE route\n");
}
function parseArgs() {
    return nopt({
        "help": Boolean,
        "version": Boolean,
        "verbose": Boolean,
        "port": Number,
        "example": String,
        "config": String,
        "print-config": Boolean,
        "with-comments": Boolean,
        "data-sources-only": Boolean,
        "file": String,
        "druid": String,
        "introspection-strategy": String
    }, {
        "v": ["--verbose"],
        "p": ["--port"],
        "c": ["--config"],
        "f": ["--file"],
        "d": ["--druid"]
    }, process.argv);
}
var parsedArgs = parseArgs();
//console.log(parsedArgs);
if (parsedArgs['help']) {
    printUsage();
    process.exit();
}
if (parsedArgs['version']) {
    console.log(packageObj.version);
    process.exit();
}
var DEFAULT_CONFIG = {
    port: 9090,
    sourceListScan: 'auto',
    sourceListRefreshInterval: 10000,
    dataSources: []
};
if (!parsedArgs['example'] && !parsedArgs['config'] && !parsedArgs['druid'] && !parsedArgs['file']) {
    printUsage();
    process.exit();
}
var exampleConfig = null;
if (parsedArgs['example']) {
    delete parsedArgs['druid'];
    var example = parsedArgs['example'];
    if (example === 'wiki') {
        try {
            exampleConfig = index_2.loadFileSync(path.join(__dirname, "../../config-example-" + example + ".yaml"), 'yaml');
        }
        catch (e) {
            errorExit("Could not load example config for '" + example + "': " + e.message);
        }
    }
    else {
        console.log("Unknown example '" + example + "'. Possible examples are: wiki");
        process.exit();
    }
}
var configFilePath = parsedArgs['config'];
var config;
if (configFilePath) {
    try {
        config = index_2.loadFileSync(configFilePath, 'yaml');
    }
    catch (e) {
        errorExit("Could not load config from '" + configFilePath + "': " + e.message);
    }
}
else {
    config = DEFAULT_CONFIG;
}
// If there is an example config take its dataSources
if (exampleConfig && Array.isArray(exampleConfig.dataSources)) {
    config.dataSources = exampleConfig.dataSources;
}
// If a file is specified add it as a dataSource
var file = parsedArgs['file'];
if (file) {
    config.dataSources.push({
        name: path.basename(file, path.extname(file)),
        engine: 'native',
        source: file
    });
}
exports.PRINT_CONFIG = Boolean(parsedArgs['print-config']);
exports.START_SERVER = !exports.PRINT_CONFIG;
exports.VERBOSE = Boolean(parsedArgs['verbose'] || config.verbose);
exports.PORT = parseInt(parsedArgs['port'] || config.port, 10);
exports.DRUID_HOST = parsedArgs['druid'] || config.druidHost;
exports.TIMEOUT = parseInt(config.timeout, 10) || 30000;
exports.INTROSPECTION_STRATEGY = String(parsedArgs["introspection-strategy"] || config.introspectionStrategy || 'segment-metadata-fallback');
exports.SOURCE_LIST_SCAN = exports.START_SERVER ? config.sourceListScan : 'disable';
exports.SOURCE_LIST_REFRESH_INTERVAL = exports.START_SERVER ? (parseInt(config.sourceListRefreshInterval, 10) || 10000) : 0;
exports.HIDE_GITHUB_ICON = Boolean(config.hideGitHubIcon);
exports.HEADER_BACKGROUND = config.headerBackground || null;
if (exports.SOURCE_LIST_REFRESH_INTERVAL && exports.SOURCE_LIST_REFRESH_INTERVAL < 1000) {
    errorExit('can not refresh more often than once per second');
}
exports.DATA_SOURCES = (config.dataSources || []).map(function (dataSourceJS, i) {
    if (typeof dataSourceJS !== 'object')
        errorExit("DataSource " + i + " is not valid");
    var dataSourceName = dataSourceJS.name;
    if (typeof dataSourceName !== 'string')
        errorExit("DataSource " + i + " must have a name");
    // Convert maxTime into refreshRule if a maxTime exists
    if (dataSourceJS.maxTime && (typeof dataSourceJS.maxTime === 'string' || dataSourceJS.maxTime.toISOString)) {
        dataSourceJS.refreshRule = { rule: 'fixed', time: dataSourceJS.maxTime };
        console.warn('maxTime found in config, this is deprecated please convert it to a refreshRule like so:', dataSourceJS.refreshRule);
        delete dataSourceJS.maxTime;
    }
    try {
        return index_1.DataSource.fromJS(dataSourceJS);
    }
    catch (e) {
        errorExit("Could not parse data source '" + dataSourceJS.name + "': " + e.message);
    }
});
var druidRequester = null;
if (exports.DRUID_HOST) {
    druidRequester = index_2.properDruidRequesterFactory({
        druidHost: exports.DRUID_HOST,
        timeout: exports.TIMEOUT,
        verbose: exports.VERBOSE,
        concurrentLimit: 5
    });
}
var fileDirectory = path.join(__dirname, '../..');
exports.DATA_SOURCE_MANAGER = index_2.dataSourceManagerFactory({
    dataSources: exports.DATA_SOURCES,
    druidRequester: druidRequester,
    dataSourceFiller: index_2.dataSourceFillerFactory(druidRequester, fileDirectory, exports.TIMEOUT, exports.INTROSPECTION_STRATEGY),
    sourceListScan: exports.SOURCE_LIST_SCAN,
    sourceListRefreshInterval: exports.SOURCE_LIST_REFRESH_INTERVAL,
    log: exports.PRINT_CONFIG ? null : function (line) { return console.log(line); }
});
if (exports.PRINT_CONFIG) {
    var withComments = Boolean(parsedArgs['with-comments']);
    var dataSourcesOnly = Boolean(parsedArgs['data-sources-only']);
    exports.DATA_SOURCE_MANAGER.getQueryableDataSources().then(function (dataSources) {
        var lines = [
            ("# generated by Pivot version " + exports.VERSION),
            ''
        ];
        if (!dataSourcesOnly) {
            if (exports.VERBOSE) {
                if (withComments) {
                    lines.push("# Run Pivot in verbose mode so it prints out the queries that it issues");
                }
                lines.push("verbose: true", '');
            }
            if (withComments) {
                lines.push("# The port on which the Pivot server will listen on");
            }
            lines.push("port: " + exports.PORT, '');
            if (exports.DRUID_HOST) {
                if (withComments) {
                    lines.push("# A Druid broker node that can serve data (only used if you have Druid based data source)");
                }
                lines.push("druidHost: " + exports.DRUID_HOST, '');
                if (withComments) {
                    lines.push("# A timeout for the Druid queries in ms (default: 30000 = 30 seconds)");
                    lines.push("#timeout: 30000", '');
                }
            }
            if (exports.INTROSPECTION_STRATEGY !== 'segment-metadata-fallback') {
                if (withComments) {
                    lines.push("# The introspection strategy for the Druid external");
                }
                lines.push("introspectionStrategy: " + exports.INTROSPECTION_STRATEGY, '');
            }
            lines.push("# Should new datasources automatically be added");
            lines.push("sourceListScan: disable", '');
        }
        if (dataSources.length) {
            lines.push('dataSources:');
            lines = lines.concat.apply(lines, dataSources.map(function (d) { return index_2.dataSourceToYAML(d, withComments); }));
        }
        else {
            lines.push('dataSources: [] # Could not find any data sources please verify network connectivity');
        }
        console.log(lines.join('\n'));
    }).done();
}
