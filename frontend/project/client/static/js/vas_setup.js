window.onload = initializeWindow;

function initializeWindow() {
  $("#delay-panel").hide();
  $("#cluster-delay-panel").hide();
  $("#gateway-delay-panel").hide();
  return;
}

function togglePanel(panel_id) {
  $(`#${panel_id}`).toggle();
  return;
}


