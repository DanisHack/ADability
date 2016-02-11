'use strict';
var Q = require('q');
var plywood_1 = require('plywood');
var index_1 = require('../../../common/models/index');

function dataSourceManagerFactory(options) {
    var dataSources = options.dataSources, dataSourceStubFactory = options.dataSourceStubFactory, druidRequester = options.druidRequester, dataSourceFiller = options.dataSourceFiller, sourceListScan = options.sourceListScan, sourceListRefreshOnLoad = options.sourceListRefreshOnLoad, sourceListRefreshInterval = options.sourceListRefreshInterval, log = options.log;
    if (!sourceListScan)
        sourceListScan = 'auto';
    if (sourceListScan !== 'disable' && sourceListScan !== 'auto') {
        throw new Error("sourceListScan must be disabled or auto is ('" + sourceListScan + "')");
    }
    if (!dataSourceStubFactory) {
        dataSourceStubFactory = function (name) {
            return index_1.DataSource.fromJS({
                name: name,
                engine: 'druid',
                source: name,
                timeAttribute: 'time',
                refreshRule: index_1.RefreshRule.query().toJS()
            });
        };
    }
    if (!log)
        log = function () { };
    var myDataSources = dataSources || [];
    function findDataSource(name) {
        for (var _i = 0; _i < myDataSources.length; _i++) {
            var myDataSource = myDataSources[_i];
            if (myDataSource.name === name)
                return myDataSource;
        }
        return null;
    }
    function getQueryable() {
        return myDataSources.filter(function (dataSource) { return dataSource.isQueryable(); });
    }
    // Updates the correct datasource (by name) in myDataSources
    function addOrUpdateDataSource(dataSource) {
        var updated = false;
        myDataSources = myDataSources.map(function (myDataSource) {
            if (myDataSource.name === dataSource.name) {
                updated = true;
                return dataSource;
            }
            else {
                return myDataSource;
            }
        });
        if (!updated) {
            myDataSources.push(dataSource);
        }
    }
    function introspectDataSource(dataSource) {
        return dataSourceFiller(dataSource).then(function (filledDataSource) {
            addOrUpdateDataSource(filledDataSource);
        }).catch(function (e) {
            log("Failed to introspect data source: '" + dataSource.name + "' because " + e.message);
        });
    }
    function loadDruidDataSources() {
        if (!druidRequester)
            return Q(null);
        return druidRequester({
            query: { queryType: 'sourceList' }
        }).then(function (ds) {
            if (!Array.isArray(ds))
                throw new Error('invalid result from data source list');
            var unknownDataSourceNames = [];
            var nonQueryableDataSources = [];
            ds.forEach(function (d) {
                var existingDataSources = myDataSources.filter(function (dataSource) {
                    return dataSource.engine === 'druid' && dataSource.source === d;
                });
                if (existingDataSources.length === 0) {
                    unknownDataSourceNames.push(d);
                }
                else {
                    nonQueryableDataSources = nonQueryableDataSources.concat(existingDataSources.filter(function (dataSource) {
                        return !dataSource.isQueryable();
                    }));
                }
            });
            nonQueryableDataSources = nonQueryableDataSources.concat(unknownDataSourceNames.map(function (name) {
                var newDataSource = dataSourceStubFactory(name);
                log("Adding Druid data source: '" + name + "'");
                addOrUpdateDataSource(newDataSource);
                return newDataSource;
            }));
            // Nothing to do
            if (!nonQueryableDataSources.length)
                return Q(null);
            return Q.allSettled(nonQueryableDataSources.map(function (dataSource) {
                return introspectDataSource(dataSource);
            }));
        }).catch(function (e) {
            log("Could not get druid source list: '" + e.message + "'");
        });
    }
    var initialTasks = [];
    myDataSources.forEach(function (dataSource) {
        initialTasks.push(introspectDataSource(dataSource));
    });
    if (sourceListScan === 'auto' && druidRequester) {
        initialTasks.push(loadDruidDataSources());
    }
    var initialLoad = Q.allSettled(initialTasks);
    initialLoad.then(function () {
        var queryableDataSources = getQueryable();
        log("Initial introspection complete. Got " + myDataSources.length + " data sources, " + queryableDataSources.length + " queryable");
    });
    if (sourceListScan === 'auto' && druidRequester && sourceListRefreshInterval) {
        log("Will refresh data sources every " + sourceListRefreshInterval + "ms");
        setInterval(loadDruidDataSources, sourceListRefreshInterval).unref();
    }
    // Periodically check if max time needs to be updated
    setInterval(function () {
        myDataSources.forEach(function (dataSource) {
            if (dataSource.shouldQueryMaxTime()) {
                index_1.DataSource.updateMaxTime(dataSource).then(function (updatedDataSource) {
                    log("Getting the latest MaxTime for '" + updatedDataSource.name + "'");
                    addOrUpdateDataSource(updatedDataSource);
                });
            }
        });
    }, 1000).unref();
    return {
        getDataSources: function () {
            return initialLoad.then(function () {
                if (myDataSources.length && !sourceListRefreshOnLoad)
                    return myDataSources;
                // There are no data sources... lets try to load some:
                return loadDruidDataSources().then(function () {
                    return myDataSources; // we tried
                });
            });
        },
        getQueryableDataSources: function () {
            return initialLoad.then(function () {
                var queryableDataSources = getQueryable();
                if (queryableDataSources.length && !sourceListRefreshOnLoad)
                    return queryableDataSources;
                // There are no data sources... lets try to load some:
                return loadDruidDataSources().then(function () {
                    return getQueryable(); // we tried
                });
            });
        },
        getQueryableDataSource: function (name) {
            return initialLoad.then(function () {
                var myDataSource = findDataSource(name);
                if (myDataSource) {
                    if (myDataSource.isQueryable())
                        return myDataSource;
                    return introspectDataSource(myDataSource).then(function () {
                        var queryableDataSource = findDataSource(name);
                        return (queryableDataSource && queryableDataSource.isQueryable()) ? queryableDataSource : null;
                    });
                }
                // There are no data sources... lets try to load some:
                return loadDruidDataSources().then(function () {
                    var queryableDataSource = findDataSource(name);
                    return (queryableDataSource && queryableDataSource.isQueryable()) ? queryableDataSource : null;
                });
            });
        }
    };
}
exports.dataSourceManagerFactory = dataSourceManagerFactory;
function dimensionToYAML(dimension) {
    var lines = [
        ("      - name: " + dimension.name),
        ("        title: " + dimension.title)
    ];
    if (dimension.kind !== 'string') {
        lines.push("        kind: " + dimension.kind);
    }
    if (!dimension.expression.equals(plywood_1.$(dimension.name))) {
        lines.push("        expression: " + dimension.expression.toString());
    }
    lines.push('');
    return lines;
}
function measureToYAML(measure) {
    var lines = [
        ("      - name: " + measure.name),
        ("        title: " + measure.title),
    ];
    var ex = measure.expression.toString();
    var comment = '';
    if (/\bmin\b|\bmax\b|\bunique\b|\buniques\b/i.test(ex.replace(/_/g, ' '))) {
        comment = ' # double check please';
    }
    lines.push("        expression: " + ex + comment);
    var format = measure.format;
    if (format !== index_1.Measure.DEFAULT_FORMAT) {
        lines.push("        format: " + format);
    }
    lines.push('');
    return lines;
}
function dataSourceToYAML(dataSource, withComments) {
    var lines = [
        ("  - name: " + dataSource.name),
        ("    title: " + dataSource.title),
        ("    engine: " + dataSource.engine),
        ("    source: " + dataSource.source),
        ""
    ];
    if (dataSource.timeAttribute) {
        if (withComments) {
            lines.push("    # The primary time attribute of the data refers to the attribute that must always be filtered on");
            lines.push("    # This is particularly useful for Druid data sources as they must always have a time filter.");
        }
        lines.push("    timeAttribute: " + dataSource.timeAttribute.name, '');
    }
    var refreshRule = dataSource.refreshRule;
    if (withComments) {
        lines.push("    # The refresh rule describes how often the data source looks for new data. Default: 'query'/PT1M (every minute)");
        lines.push("    # In this case it has to be fixed since this data source is static");
    }
    lines.push("    refreshRule:");
    lines.push("      rule: " + refreshRule.rule);
    if (refreshRule.time) {
        lines.push("      time: " + refreshRule.time.toISOString());
    }
    if (refreshRule.refresh) {
        lines.push("      refresh: " + refreshRule.refresh.toString());
    }
    lines.push('');
    var defaultTimezone = dataSource.defaultTimezone;
    if (withComments) {
        lines.push("    # The default timezone for this dataset to operate in defaults to UTC");
    }
    if (defaultTimezone.equals(index_1.DataSource.DEFAULT_TIMEZONE)) {
        if (withComments) {
            lines.push("    #defaultTimezone: " + index_1.DataSource.DEFAULT_TIMEZONE.toString(), '');
        }
    }
    else {
        lines.push("    defaultTimezone: " + defaultTimezone.toString() + "}", '');
    }
    var defaultDuration = dataSource.defaultDuration;
    if (withComments) {
        lines.push("    # The default duration for the time filter (if not set P3D is used)");
    }
    if (defaultDuration.equals(index_1.DataSource.DEFAULT_DURATION)) {
        if (withComments) {
            lines.push("    #defaultDuration: " + index_1.DataSource.DEFAULT_DURATION.toString(), '');
        }
    }
    else {
        lines.push("    defaultDuration: " + defaultDuration.toString(), '');
    }
    var defaultSortMeasure = dataSource.defaultSortMeasure;
    if (withComments) {
        lines.push("    # The default sort measure name (if not set the first measure name is used)");
    }
    lines.push("    defaultSortMeasure: " + defaultSortMeasure, '');
    var defaultPinnedDimensions = dataSource.defaultPinnedDimensions.toArray();
    if (withComments) {
        lines.push("    # The names of dimensions that are pinned by default (in order that they will appear in the pin bar)");
    }
    lines.push("    defaultPinnedDimensions: " + JSON.stringify(defaultPinnedDimensions), '');
    var introspection = dataSource.introspection;
    if (withComments) {
        lines.push("    # How the dataset should be introspected", "    # possible options are:", "    # * none - Do not do any introspection, take what is written in the config as the rule of law.", "    # * no-autofill - Introspect the datasource but do not automatically generate dimensions or measures", "    # * autofill-dimensions-only - Introspect the datasource, automatically generate dimensions only", "    # * autofill-measures-only - Introspect the datasource, automatically generate measures only", "    # * autofill-all - (default) Introspect the datasource, automatically generate dimensions and measures");
    }
    lines.push("    introspection: " + introspection, '');
    var dimensions = dataSource.dimensions.toArray();
    if (withComments) {
        lines.push("    # The list of dimensions defined in the UI. The order here will be reflected in the UI");
    }
    lines.push('    dimensions:');
    if (withComments) {
        lines.push("      # A general dimension looks like so:", "      #", "      # name: channel", "      # ^ the name of the dimension as used in the URL (you should try not to change these)", "      #", "      # title: The Channel", "      # ^ (optional) the human readable title. If not set a title is generated from the 'name'", "      #", "      # kind: string", "      # ^ (optional) the kind of the dimension. Can be 'string', 'time', 'number', or 'boolean'. Defaults to 'string'", "      #", "      # expression: $channel", "      # ^ (optional) the Plywood bucketing expression for this dimension. Defaults to '$name'", "      #   if, say, channel was called 'cnl' in the data you would put '$cnl' here", "      #   See also the expressions API reference: https://github.com/implydata/plywood/blob/master/docs/expressions.md", "");
    }
    lines = lines.concat.apply(lines, dimensions.map(dimensionToYAML));
    var measures = dataSource.measures.toArray();
    if (withComments) {
        lines.push("    # The list of measures defined in the UI. The order here will be reflected in the UI");
    }
    lines.push("    measures:");
    if (withComments) {
        lines.push("      # A general measure looks like so:", "      #", "      # name: avg_revenue", "      # ^ the name of the dimension as used in the URL (you should try not to change these)", "      #", "      # title: Average Revenue", "      # ^ (optional) the human readable title. If not set a title is generated from the 'name'", "      #", "      # expression: $main.sum($revenue) / $main.sum($volume) * 10", "      # ^ (optional) the Plywood bucketing expression for this dimension.", "      #   Usually defaults to '$main.sum($name)' but if the name contains 'min' or 'max' will use that as the aggregate instead of sum.", "      #   this is the place to define your fancy formulas", "");
    }
    lines = lines.concat.apply(lines, measures.map(measureToYAML));
    lines.push('');
    return lines;
}
exports.dataSourceToYAML = dataSourceToYAML;
