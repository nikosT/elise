# Python dependencies
import os
import socket
import sys

# Dash dependencies
from dash_extensions.WebSocket import WebSocket
import dash_bootstrap_components as dbc
from dash_extensions.enrich import Input, Output, State, callback, html, ALL
from dash.exceptions import PreventUpdate

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../..")
))

# WebUI dependencies
from webui.utils.schematic_utils import export_schematic
from webui.utils.common_utils import get_session_dir
from webui.utils.results_utils import get_experiment_results

# elise library dipendencies
from batch.submit import execute_simulation
from common.communication import get_ip

ws_ipaddr = get_ip()
ws_url = f"ws://{ws_ipaddr}:55500"
app_progress_report = WebSocket(id="app-progress-report", url=ws_url)
progress_finished = dbc.Alert("The simulation has finished", id="execute-simulation-alert", duration=5000, is_open=False)
progress_bar = dbc.Alert([
    html.H2("Simulation Progress"),
    dbc.Progress(id="progress-bar", value=0)
    ],
    id="progress-bar-collapse",
    is_open=False, 
    # style={"position": "fixed", "bottom": "0", "left": "0", "right": "0", "margin": "0", "width": "100%"}
)

@callback(
    Output("progress-bar", "value"),
    Output("progress-bar", "label"),
    Input("app-progress-report", "message"),
    prevent_initial_call=True
)
def update_progress_store(msg):
    data_str = str(msg["data"])
    data_str = data_str.rstrip("\x00")
    progress = int(float(data_str))
    return progress, f"{progress} %"

@callback(
    Output("progress-bar-collapse", "is_open"),
    Output("progress-bar-collapse", "duration"),
    Input("execute-simulation-btn", "n_clicks"),
    prevent_initial_call=True
)
def webui_show_progress(n_clicks):
    return True, None

@callback(
    Output("app-results-store", "data"),
    Output("execute-simulation-alert", "is_open"),
    Output("progress-bar-collapse", "duration", allow_duplicate=True),
    Input("execute-simulation-btn", "n_clicks"),
    State("app-sim-schematic", "data"),
    State("app-session-store", "data"),
    State("app-results-store", "data"),
    State({"check-item": "action", "index": ALL}, "value"),
    State("main-action-multiprocessing-provider", "value"),
    prevent_initial_call=True
)
def webui_execute_simulation(n_clicks, schematic_data, session_data, results_data, enabled_actions, provider):
    if not n_clicks:
        raise PreventUpdate
    
    # Filter out unchecked actions
    filtered_actions = {name: val for i, [name, val] in enumerate(schematic_data["actions"].items()) if enabled_actions[i]}
    schematic_data["actions"] = filtered_actions
    
    # Prepare the schematic
    filename = export_schematic(schematic_data, session_data)
    # Get provider
    match provider:
        case "Open MPI":
            provider = "openmpi"
        case "Intel MPI":
            provider = "intelmpi"
        case "Python":
            provider = "mp"
    # Prepare the command line
    cmdline = ["-f", filename, "-p", provider, "--webui", "--export_reports", f"{get_session_dir(session_data)}/results"]
    # Execute the simulation
    execute_simulation(cmdline)
    # Get the results for the simulation run
    results = get_experiment_results(schematic_data, session_data)
    results_data.append(results)

    return results_data, True, 3000