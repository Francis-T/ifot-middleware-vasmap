window.onload = initializeWindow;

function initializeWindow() {
  $("#delay-panel").hide();
  $("#cluster-delay-panel").hide();
  $("#gateway-delay-panel").hide();
  $("#simulation-setup-form").submit( function () {
    let rsu_count = $("#v_rsu_count").val();
    let master_count = $("#select_master_number :selected").val();
    let worker_count = $("#select_worker_number :selected").val();

    console.log(rsu_count);
    console.log(master_count);
    console.log(worker_count);

    if ( (master_count * worker_count) <= rsu_count ) {
      console.log("PASS");
      return;
    }

    let msg_display_elem = $("#simulation-setup-form .message-display");
    msg_display_elem.addClass("alert alert-warning");

    let msg = 'RSUs per cluster would exceed the total number of RSUs: <br/>';
    msg += `(${worker_count} x ${master_count} > ${rsu_count})`;
    msg_display_elem.html(msg);

    return false;
  });
  return;
}

function togglePanel(panel_id) {
  $(`#${panel_id}`).toggle();
  return;
}


