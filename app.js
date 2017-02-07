var express = require('express')
var app = express()
var sqlite3 = require('sqlite3');
var async = require('async')
var redis = require('redis');
var client = redis.createClient(); //creates a new client
var crypto = require('crypto');
var db;
function SetValue(name, value, expire_in_s) {
    client.set(name + "", value + "");
    client.expire(name, expire_in_s);
}


function GetValue(name, callback) {
    var ret_err;
    var ret_data;
    client.get(name, function (e, d) {
        if (e) {
            log.error("Error: " + e);
            ret_err = e;
        } else {
            ret_data = d
        }
        callback(ret_err, ret_data);
    });
}

function InitDB() {
    db = new sqlite3.Database('lolmh_test');
}

app.get('/', function (req, res) {
    res.send('Hello World!')

});
app.get('/paging/:order_field/:order_value/:direction/:ref/:per_page', function (req, res) {
    var direction = req.params.direction
    var ref = req.params.ref
    var order = req.params.order_value
    var order_fieled = req.params.order_field
    var per_page = req.params.per_page
    console.log(`Direction = ${direction}, ref = ${ref}`);
    console.log("Direction = %s, ref = %s", req.params.direction, req.params.ref)
    var sql_select = "select * from match";
    var sql_hash_signature = crypto.createHash('md5').update(sql_select).digest('hex')
    var condition = {field: "summoner_id", operator: "=", value: "37179679"};
    var filter_field = 'summoner_id'
    var filter_sign = "=";
    var filter_value = '37179679';
    var sql_with_filters = `SELECT * FROM (${sql_select}) where ${condition.field} ${condition.operator} ${condition.value} ORDER BY ${order_fieled} ${order}`
    var sql_with_filters_count = `SELECT count(${order_fieled}) as rows_count from(${sql_with_filters})`
    var rows_with_filter_count = 0
    var sql_with_filters_and_paging;
    var direction_operators = {next: "<", prev: ">", startRows: "<=", endRows: ">="}
    var direction_operators_inverse = {next: ">", prev: "<", startRows: "<=", endRows: ">="}
    var direction_operators_picked;
    if (order.toLowerCase() == 'asc') {
        direction_operators_picked = direction_operators_inverse
        if (direction == 'endRows' || direction == 'prev') {
            order = 'desc';
        }
    } else {
        direction_operators_picked = direction_operators
        if (direction == 'endRows' || direction == 'prev') {
            order = 'asc';
        }
    }
    var paging_func = async.seq(
            function (done) {
                GetValue(`count:${sql_hash_signature}_${order_fieled}_${order}_${direction}_${ref}_${per_page}`, HandleGetCountFromRedis);
                function HandleGetCountFromRedis(err, data) {
                    console.log(`Count from redis error: ${err}, data: ${data}`)
                    if (err) {
                        done(null)
                    } else {
                        rows_with_filter_count = data
                        done(null)
                    }
                }
            },
            function (done) {
                if (rows_with_filter_count) {
                    done(null)
                } else {
                    db.get(sql_with_filters_count, function (err, row) {
                        if (err) {
                            done("Error selecting count from table: ", err)
                        } else {
                            console.log("Count row: ", row)
                            rows_with_filter_count = row.rows_count
                            SetValue(`count:${sql_hash_signature}_${order_fieled}_${order}_${direction}_${ref}_${per_page}`, rows_with_filter_count, 60)
                            done(null);
                        }
                    })
                }
            }
    );
    paging_func(HandlePagingFunction);
    function HandlePagingFunction(err, data) {
        console.log("Error", err, " rows with filter count: ", rows_with_filter_count)
    }
    if (direction && ref) {
        sql_with_filters_and_paging = `SELECT * FROM (${sql_with_filters}) where ${order_fieled} ${direction_operators_picked[direction]} ${ref} LIMIT ${per_page}`

    }
    console.log(`Sql with filters: ${sql_with_filters}`);
    console.log(`Sql with filters and paging: ${sql_with_filters_and_paging}`)
    db.all(sql_with_filters_and_paging, function (err, rows) {
        console.log(`Number of rows ${rows.length}`)
        rows.forEach(function (m) {
            console.log(`Match: `, m)
        })
    });
})

function GetPagedRowsFromSelect(select_query, order_field, order_value, direction, ref, per_page, callback) {
    var defined_directions = ['next', 'prev', 'nextIncl', 'prevIncl']
    var direction = direction
    var ref = ref
    var p_order_value = order_value
    var order_fieled = order_field
    var per_page = per_page
    var sql_select = select_query;
    var sql_hash_signature = crypto.createHash('md5').update(sql_select).digest('hex')
    var condition = {field: "summoner_id", operator: "=", value: "37179679"};

    var rows_with_filter_count = 0
    var sql_with_filters_and_paging;
    if (defined_directions.indexOf(direction) == -1) {
        var ret_data
        return callback("Unknown direction", ret_data)
    }
    var direction_operators = {next: "<", prev: ">", nextIncl: "<=", prevIncl: ">="}
    var direction_operators_inverse = {next: ">", prev: "<", nextIncl: "<=", prevIncl: ">="}
    var direction_operators_picked;
    if (p_order_value.toLowerCase() == 'asc') {
        direction_operators_picked = direction_operators_inverse
        if (direction == 'prev' || direction == 'prevIncl') {
            p_order_value = 'desc';
        }
    } else {
        direction_operators_picked = direction_operators
        if (direction == 'prev' || direction == 'prevIncl') {
            p_order_value = 'asc';
        }
    }

    var sql_with_filters = `SELECT * FROM (${sql_select}) where ${condition.field} ${condition.operator} ${condition.value} ORDER BY ${order_fieled} ${p_order_value}`
    var sql_with_filters_count = `SELECT count(${order_fieled}) as rows_count from(${sql_with_filters})`

    console.log(`Order value: ${p_order_value}`)

    if (direction && ref) {
        sql_with_filters_and_paging = `SELECT * FROM (${sql_with_filters}) where ${order_fieled} ${direction_operators_picked[direction]} ${ref} LIMIT ${per_page}`

    }
    console.log(`Sql with filters: ${sql_with_filters}`);
    console.log(`Sql with filters and paging: ${sql_with_filters_and_paging}`)
    var pagin_func = async.seq(
            function (done) {
                GetValue(`count:${sql_hash_signature}_${order_fieled}_${order_value}_${direction}_${ref}_${per_page}`, HandlePaginFuncRedis)

                function HandlePaginFuncRedis(err, data) {
                    var undef;
                    if (err) {
                        done(null, undef)
                    } else {
                        if (data && data.length > 0) {
                            done(null, JSON.parse(data))
                        } else {
                            done(null, undef)
                        }
                    }
                }
            },
            function (data, done) {
                if (data) {
                    done(null, data)
                } else {
                    db.all(sql_with_filters_and_paging, function (err, rows) {
                        console.log(`Number of rows ${rows.length}`)
                        var ret_err;
                        var ret_data;
                        if (rows.length > 0) {
                            if (direction == 'prev' || direction == 'prevIncl') {
                                rows = rows.reverse();
                            }
                            rows.forEach(function (m) {
                                console.log(`Match: `, m)
                            })
                            ret_data = rows
                            SetValue(`count:${sql_hash_signature}_${order_fieled}_${order_value}_${direction}_${ref}_${per_page}`, JSON.stringify(ret_data), 60)
                            done(ret_err, ret_data)
                        } else {
                            if (direction == 'next') {
                                GetPagedRowsFromSelect(select_query, order_field, order_value, 'prevIncl', ref, per_page, done)
                            } else if (direction == 'prev') {
                                GetPagedRowsFromSelect(select_query, order_field, order_value, 'nextIncl', ref, per_page, done)
                            } else {
                                done(ret_err, ret_data)
                            }
                        }
                    });
                }
            }
    )
    pagin_func(callback)
}

app.get('/test', function (req, res) {
    var ret_rows = []
    var count = 0
    var ret_err
    //GetPagedRowsFromSelect("select * from match", "create_date", "desc", "next", "1486042324894", 3, test);
    GetPagedRowsFromSelect("select * from match", "create_date", "desc", "prev", "1486042324894", 3, function (err, data) {
        test(err, data, 1)
    });
    GetPagedRowsFromSelect("select * from match", "create_date", "desc", "next", "1486042324894", 3, function (err, data) {
        test(err, data, 2)
    });
    GetPagedRowsFromSelect("select * from match", "create_date", "desc", "prev", "1485605880385", 3, function (err, data) {
        test(err, data, 3)
    });
    GetPagedRowsFromSelect("select * from match", "create_date", "desc", "next", "1481981405320", 3, function (err, data) {
        test(err, data, 4)
    });

    function test(err, data, num) {
        console.log(`Error: ${err}, data: `, data);
        if (!ret_err)
            if (err) {
                ret_err = err
                res.send(err)
                return
            } else {
                var row_create_dates = []
                for (var i = 0; i < data.length; i++) {
                    row_create_dates.push(data[i].create_date)
                }
                ret_rows.push({num: num, data: row_create_dates})

                count++
                if (count >= 4) {


                    res.send(ret_rows)
                }
            }
    }
})

app.listen(4000, function () {
    console.log('Example app listening on port 4000!')
    InitDB()
})