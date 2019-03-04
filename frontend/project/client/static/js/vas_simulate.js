var strategy = "";
var rsu_list = null;
var cluster_data = null;
var svc_params = null;
var socket = null;
var recvd_results = {};

window.onload = initializeWindow;

function initializeWindow() {
  console.log('Connection Target: http://' + document.domain + ':' + location.port);
  socket = io.connect('http://' + document.domain + ':' + location.port);
  socket.on('connect', function() {
    socket.emit('my event', {data: 'vas_simulate: I\'m connected!'});
    console.log("Connected");

    requestAverageSpeeds();

    return;
  });

  socket.on('results', function(data) {
    console.log(data);
    result_data = JSON.parse(data);

    console.log(`Result received: ${result_data.type} ${result_data.subtype}`);
    if (recvd_results[result_data.subtype]) {
      recvd_results[result_data.subtype] += 1;
    } else {
      recvd_results[result_data.subtype] = 1;
    }

    let result_str = "";
    let k = "";
    for (k in recvd_results) {
      result_str += `<div>${k} : ${recvd_results[k]}</div>`;
    }
    $("#result-progress").html(result_str);

    if ((result_data.type === "result") && (result_data.subtype === "aggregation")) {
      displayAverageSpeedData(result_data.results.result);
      requestExecTimeData(result_data.results.unique_id);
      $("#result-progress").addClass("border");
    }

    console.log("Finished");
    return;
  });

  console.log("Test");
  
  strategy      = JSON.parse( $("#v_strategy").val() );
  rsu_list      = JSON.parse( $("#v_rsu_list").val() );
  cluster_data  = JSON.parse( $("#v_cluster_data").val() );
  svc_params    = JSON.parse( $("#v_svc_params").val() );
  delay_profile = JSON.parse( $("#v_delay_profile").val() );

  console.log(strategy);
  console.log(rsu_list);
  console.log(cluster_data);
  console.log(svc_params);


  return;
}

function requestAverageSpeeds() {
    let request_data = new FormData();

    //request_data.append('split_count'     , parseInt($("#nmb_split_count").val()) );
    request_data.append('host'            , svc_params.host );
    request_data.append('port'            , svc_params.port );
    request_data.append('db_name'         , svc_params.db_name );
    request_data.append('db_ret_policy'   , svc_params.db_ret_policy );
    request_data.append('db_meas'         , svc_params.db_meas );
    request_data.append('start_time'      , svc_params.start_time );
    request_data.append('end_time'        , svc_params.end_time );
    request_data.append('rsu_list'        , JSON.stringify(rsu_list) );
    request_data.append('cluster_data'    , JSON.stringify(cluster_data) );
    request_data.append('delay_profile'   , JSON.stringify(delay_profile) );
    request_data.append('strategy'        , strategy );

    $.ajax({
        url : "api/vas/get_average_speeds",
        data : request_data,
        method : "POST",
        contentType : false,
        processData : false,
        success : function(data) {
            results = { 'topic' : data.response_object.unique_ID + '/results' };
            socket.emit('subscribe', results);
            console.log(data);
            recvd_results = {};

            return;
        },
        beforeSend : function() {
            $("#btn_query_speeds").attr("disabled", true);
        },
    }).always(function (){
            $("#btn_query_speeds").attr("disabled", false);
    });
    return;
}

function requestExecTimeData(unique_id) {
    $.ajax({
        url : `api/get_exec_time/${unique_id}`,
        method : "GET",
        success : function(data) {
            displayExecTimeData(unique_id, data['exec_time_logs'][unique_id]);
            $("#tbl_timing_info").removeClass("d-none");
            return;
        },
    });
    return;
}

function displayExecTimeData(unique_id, result) {
    /* Prepare the bar chart data */
    let chart_labels = [];
    let start_time_list = [];
    let proc_time_list = [];
    let earliest_start = Infinity;
    let latest_end = 0;

    /* Get the earliest start time */
    let table = document.getElementById("tbl_timing_info_body");
    table.innerHTML = "";
    for (task of result) {
        base_start = (parseFloat(task.start_time) / 1000000.0);
        if (base_start < earliest_start) {
            earliest_start = base_start;
        }
    }

    for (task of result) {
        let tbl_row = document.createElement("tr");
        let tbl_operation = document.createElement("td");
        let tbl_start_time = document.createElement("td");
        let tbl_end_time = document.createElement("td");
        let tbl_duration = document.createElement("td");

        /* Fill in the <td> elements */
        tbl_operation.innerHTML = task.operation;
        tbl_start_time.innerHTML = ((task.start_time / 1000000.0) - earliest_start).toFixed(6);
        tbl_end_time.innerHTML = ((task.end_time / 1000000.0) - earliest_start).toFixed(6);
        tbl_duration.innerHTML = task.duration;

        /* Put <td> elements in <tr> */
        tbl_row.appendChild(tbl_operation);
        tbl_row.appendChild(tbl_start_time);
        tbl_row.appendChild(tbl_end_time);
        tbl_row.appendChild(tbl_duration);

        /* Put <tr> in the table body */
        table.appendChild(tbl_row);
    }

    return;
}


function displayAverageSpeedData(result, is_complete) {
    let data = result;

    let rsu_key_list = Object.keys(data);
    let table = document.getElementById("tbl_results_body");

    if (is_complete) {
      /* Clear the table first */
      table.innerHTML = "";
    }

    let max_speed = 0;
    let min_speed = 999;

    for (rsu_key of rsu_key_list) {
        let speed = data[rsu_key];
        if (speed > max_speed) {
            max_speed = speed;
        }

        if (speed < min_speed) {
            min_speed = speed;
        }
    }

    for (rsu_key of rsu_key_list) {
        let tbl_row = document.createElement("tr");
        let tbl_rsu_id = document.createElement("td");
        let tbl_speed = document.createElement("td");

        /* Fill in the <td> elements */
        tbl_rsu_id.innerHTML = rsu_key;
        tbl_rsu_id.classList.add("mx-2");
        let speed = data[rsu_key];
        tbl_speed.innerHTML = speed.toFixed(2) + " kph";
        tbl_speed.classList.add("mx-2");

        /* Put <td> elements in <tr> */
        tbl_row.appendChild(tbl_rsu_id);
        tbl_row.appendChild(tbl_speed);

        /* Put <tr> in the table body */
        table.appendChild(tbl_row);
    }

    console.log(`Max: ${max_speed}, Min: ${min_speed}`);
    $("#tbl_results").removeClass("d-none");
    return;
}

