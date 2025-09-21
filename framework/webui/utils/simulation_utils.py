# Python dependencies
import json
import os
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

ws_ipaddr = "127.0.0.1"
ws_port = 55501
ws_url = f"ws://{ws_ipaddr}:{ws_port}"
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
    """Callback to update the progress bar if a message arrived from the progress server

    Parameters
    ----------
    msg :   bytes, Input
            Data sent by the progress server
    
    Returns
    -------
    tuple[int, str]
        The value and the label of the progress bar
    """
    data_str = str(msg["data"])
    data_str = data_str.rstrip("\x00")
    data = json.loads(data_str)
    progress = int(data["progress"])
    return progress, f"{progress} %"


@callback(
    Output("progress-bar-collapse", "is_open"),
    Output("progress-bar-collapse", "duration"),
    Input("execute-simulation-btn", "n_clicks"),
    prevent_initial_call=True
)
def webui_show_progress(n_clicks):
    """Callback to display the progress bar

    Parameters
    ----------
    n_clicks    :   int, Input
                    The triggering event by click the dbc.Button with id='execute-simulation-btn' that starts the simulation.

    Returns
    -------
    True, Any
        Opens the progress bar for an infinite amount of time
    """
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
    """Callback to execute a batch of simulation configurations

    Parameters
    ----------
        n_clicks        :   int, Input
                            The triggering event by click the dbc.Button with id='execute-simulation-btn' that starts the simulation.
        
        schematic_data  :   dict, State
                            The dcc.Store that stores the schematic data for this batch.
        
        session_data    :   dict, State
                            The dcc.Store that has information about the current web session.

        results_data    :   dict, State
                            The dcc.Store that stores the results of all the simulation runs.
        
        enabled_actions :   list, State
                            The list of enabled postprocessing actions.
        
        provider        :   str, State
                            The provider created for launching the parallel workers.

    Returns
    -------
    tuple[dict, bool, int]
        Returns the new results, opens the Alert component notifying that the simulation has ended and hides the progress bar after an integer amount of milliseconds
    """

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