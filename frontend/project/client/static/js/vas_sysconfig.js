var toggle_panels = [ 'add-rsu-panel', 'generate-rsu-panel', 'generate-rsu-data-panel' ];
window.onload = initializeWindow;

function initializeWindow() {
  $("#add-rsu-panel").hide();
  $("#generate-rsu-panel").hide();
  $("#generate-rsu-data-panel").hide();
  //$("#cluster-delay-panel").hide();
  //$("#gateway-delay-panel").hide();
  $("#tbl-rsu-info-1-cb").change( function () {
    toggleAllCheckboxes("tbl_rsu_info_1_body", this.checked);
  });
  $("#tbl-rsu-info-2-cb").change( function () {
    toggleAllCheckboxes("tbl_rsu_info_2_body", this.checked);
  });

  $("#tbl_rsu_info_1_body :checkbox").change( function () {
    if (this.checked === false) {
      $("#tbl-rsu-info-1-cb").prop("checked", false);
    }
  });

  $("#tbl_rsu_info_2_body :checkbox").change( function () {
    if (this.checked === false) {
      $("#tbl-rsu-info-2-cb").prop("checked", false);
    }
  });

  $("#generate-rsu-data-form").submit( function(event) {
    let selected_rsus = getSelectedRsuList();
    $("#v_generate_data_rsu_list").attr('value', JSON.stringify( selected_rsus ));

    return;
  });

  return;
}

function toggleAllCheckboxes(parent_element, is_checked) {
  $(`#${parent_element} :checkbox`).prop("checked", is_checked);
}

function togglePanel(panel_id) {
  $.each(toggle_panels, function (K, V) {
    if (panel_id !== V) {
      $(`#${V}`).hide();
    }
  });
  $(`#${panel_id}`).toggle();
  return;
}

function getSelectedRsuList() {
  /* Retrieve a list of all selected RSUs */
  let selected_rsus = [];
  selected_rsus = selected_rsus.concat($("#tbl_rsu_info_1_body :checked").map( function () { 
    return this.id.replace("-cb", "").toUpperCase();
  }).get());
  selected_rsus = selected_rsus.concat($("#tbl_rsu_info_2_body :checked").map( function () { 
    return this.id.replace("-cb", "").toUpperCase();
  }).get());

  return selected_rsus;
}

function clearRsuData() {
  $.ajax({
    url : 'clear_rsu_speed_data',
    method : 'POST',
    success : function () { 
      location.reload();
      return;
    },
  });
}
function clearRsus() {
  $.ajax({
    url : 'clear_rsu_data',
    method : 'POST',
    success : function () { 
      location.reload();
      return;
    },
  });
}


