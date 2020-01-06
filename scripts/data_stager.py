import os
import sys
import argparse
import logging
import sqlite3
import json
import socket
import webbrowser
import globus_sdk
from zstash.hpss import hpss_get
from zstash.extract import multiprocess_extract, extractFiles
from zstash.settings import config, DB_FILENAME, logger


hostname_endpoint = {
    "blues": "57b72e31-9f22-11e8-96e1-0a6d4e044368",
    "cori": "9d6d99eb-6d04-11e5-ba46-22000b92c6ec"
}

name_endpoint = {
    "anvil": "57b72e31-9f22-11e8-96e1-0a6d4e044368",
    "blues": "57b72e31-9f22-11e8-96e1-0a6d4e044368",
    "cori": "9d6d99eb-6d04-11e5-ba46-22000b92c6ec",
    "compy": "68fbd2fa-83d7-11e9-8e63-029d279f7e24"
}

patterns = {
    "ice": ["^mpascice.hist.am.timeSeriesStats*",
            "^mpassi.hist.am.timeSeriesStats*"],
    "ocean": "^mpaso.hist.am.timeSeriesStats*",
    "atm": "*.cam.h0.*",
    "lnd": "*.clm2.h0.*",
    "river": "*mosart.h0*",
    "restart": "*mpaso.rst.*",
    "namelist": ["*mpas-o_in", "*mpaso_in"]
}


client_id = "41808cb4-f058-48ed-8974-841d1350bd98"
redirect_uri = "https://auth.globus.org/v2/web/auth-code"
scopes = ("openid email profile "
          "urn:globus:auth:scope:transfer.api.globus.org:all")


get_input = getattr(__builtins__, "raw_input", input)


def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, "r") as f:
        tokens = json.load(f)
    return tokens


def save_tokens_to_file(filepath, tokens):
    """Save a set of tokens for later use."""
    with open(filepath, "w") as f:
        json.dump(tokens, f)


def is_remote_session():
    return os.environ.get("SSH_TTY", os.environ.get("SSH_CONNECTION"))


def do_native_app_authentication(client_id, redirect_uri,
                                 requested_scopes=None):
    """
    Does a Native App authentication flow and returns a
    dict of tokens keyed by service name.
    """
    client = globus_sdk.NativeAppAuthClient(client_id=client_id)
    client.oauth2_start_flow(
            requested_scopes=requested_scopes,
            redirect_uri=redirect_uri,
            refresh_tokens=True)
    url = client.oauth2_get_authorize_url()
    print("Native App Authorization URL: \n{}".format(url))
    if not is_remote_session():
        webbrowser.open(url, new=1)
    auth_code = get_input("Enter the auth code: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    # return a set of tokens, organized by resource server name
    return token_response.by_resource_server


def get_native_app_authorizer(client_id):
    tokens_path = os.path.join(os.path.expanduser("~"), ".globus-tokens.json")
    tokens = None
    try:
        tokens = load_tokens_from_file(tokens_path)
    except:
        pass

    if not tokens:
        tokens = do_native_app_authentication(
                client_id=client_id,
                redirect_uri=redirect_uri,
                requested_scopes=scopes)
        try:
            save_tokens_to_file(tokens_path, tokens)
        except:
            pass

    transfer_tokens = tokens["transfer.api.globus.org"]

    auth_client = globus_sdk.NativeAppAuthClient(client_id=client_id)

    return globus_sdk.RefreshTokenAuthorizer(
            transfer_tokens["refresh_token"],
            auth_client,
            access_token=transfer_tokens["access_token"],
            expires_at=transfer_tokens["expires_at_seconds"])


def main(args):

    # Determine source and destination Globus endpoints and directories
    source_endpoint = args.source
    if not source_endpoint:
        hostname = socket.gethostname()
        source_endpoint = None
        for h, ep in hostname_endpoint.items():
            if hostname.startswith(h):
                source_endpoint = ep
                break
    if not source_endpoint:
        logger.error("The source Globus endpoint is required")
        sys.exit(1)

    try:
        destination_endpoint, destination_dir = args.destination.split(":", 1)
    except ValueError:
        logger.error("Globus destination endpoint and path are incorrect")
        sys.exit(1)
    for name, ep in name_endpoint.items():
        if destination_endpoint == name:
            destination_endpoint = ep
            break

    # Obtain Globus tokens and store them in ~/.globus-tokens.json
    authorizer = get_native_app_authorizer(client_id=client_id)
    tc = globus_sdk.TransferClient(authorizer=authorizer)

    # Try to activate source and destination Globus endpoints
    resp = tc.endpoint_autoactivate(source_endpoint, if_expires_in=36000)
    if resp["code"] == "AutoActivationFailed":
        logger.error("The source endpoint is not active. Please go to https://app.globus.org/file-manager/collections/{} to activate the endpoint."
                     .format(source_endpoint))
        sys.exit(1)
    logger.info("The source Globus endpoint has been activated")

    resp = tc.endpoint_autoactivate(destination_endpoint, if_expires_in=36000)
    if resp["code"] == "AutoActivationFailed":
        logger.error("The destination endpoint is not active. Please go to https://app.globus.org/file-manager/collections/{} to activate the endpoint."
                     .format(source_endpoint))
        sys.exit(1)
    logger.info("The destination Globus endpoint has been activated")

    # Load pattern file if provided
    global patterns
    if args.pattern_file:
        with open(args.pattern_file, "r") as f:
            patterns = json.load(f)

    components = []
    if args.component:
        components = args.component.split(",")

    # Data file patterns
    file_patterns = []
    for c in components:
        p = patterns.get(c)
        if isinstance(p, str):
            file_patterns.append(p)
        elif isinstance(p, list):
            file_patterns = file_patterns + p
    file_patterns = file_patterns + args.files
    if not file_patterns:
        file_patterns = ["*"]
    logger.debug("File patterns: {}".format(file_patterns))

    # Restart file patterns
    p = patterns.get("restart")
    if isinstance(p, str):
        restart_patterns = [p]
    elif isinstance(p, list):
        restart_patterns = p
    logger.debug("Restart file patterns: {}".format(restart_patterns))

    # Namelist file patterns
    p = patterns.get("namelist")
    if isinstance(p, str):
        namelist_patterns = [p]
    elif isinstance(p, list):
        namelist_patterns = p
    logger.debug("Namelist file patterns: {}".format(namelist_patterns))

    # Download and open database
    logger.info('Opening index database')
    config.hpss = args.zstash
    hpss_get(config.hpss, DB_FILENAME)
    con = sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Retrieve some configuration settings from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)
    config.keep = bool(int(config.keep))

    # The command line arg should always have precedence
    config.keep = True
    if args.zstash is not None:
        config.hpss = args.zstash

    logger.info("Local path: {}".format(config.path))
    logger.info("HPSS path: {}".format(config.hpss))
    logger.info("Max size: {}".format(config.maxsize))

    # Find matching files
    file_matches = []
    for p in file_patterns:
        cur.execute(u"select * from files where name GLOB ? or tar GLOB ?", (p, p))
        file_matches = file_matches + cur.fetchall()

    restart_matches = []
    for p in restart_patterns:
        cur.execute(u"select * from files where name GLOB ? or tar GLOB ? limit 1", (p, p))
        restart_matches = cur.fetchall()
        if restart_matches:
            break

    namelist_matches = []
    for p in namelist_patterns:
        cur.execute(u"select * from files where name GLOB ? or tar GLOB ? limit 1", (p, p))
        namelist_matches = cur.fetchall()
        if namelist_matches:
            break

    logger.debug("Matching files: {}".format(file_matches))
    logger.debug("Matching restart file: {}".format(restart_matches))
    logger.debug("Matching namelist file: {}".format(namelist_matches))

    matches = file_matches + restart_matches + namelist_matches

    # Sort by the filename, tape (so the tar archive), and order within tapes (offset).
    matches.sort(key=lambda x: (x[1], x[5], x[6]))

    """
    Based off the filenames, keep only the last instance of a file.
    This is because we may have different versions of the same file across many tars.
    """
    insert_idx, iter_idx = 0, 1
    for iter_idx in range(1, len(matches)):
        # If the filenames are unique, just increment insert_idx.
        # iter_idx will increment after this iteration.
        if matches[insert_idx][1] != matches[iter_idx][1]:
            insert_idx += 1
        # Always copy over the value at the correct location.
        matches[insert_idx] = matches[iter_idx]

    matches = matches[:insert_idx+1]
    logger.info("{} matching files including restart and namelist files".format(len(matches)))

    # Sort by tape and offset, so that we make sure that extract the files by tape order.
    matches.sort(key=lambda x: (x[5], x[6]))

    # Retrieve from tapes
    if args.workers > 1:
        logger.debug("Running zstash with multiprocessing")
        failures = multiprocess_extract(args.workers, matches, True, True)
    else:
        failures = extractFiles(matches, True)

    # Close database
    logger.debug('Closing index database')
    con.close()

    if failures:
        logger.error("Encountered an error for files:")
        for fail in failures:
            logger.error("{} in {}".format(fail[1], fail[5]))
        broken_tars = sorted(set([f[5] for f in failures]))
        logger.error("The following tar archives had errors:")
        for tar in broken_tars:
            logger.error(tar)
        sys.exit(1)

    # Create a manifest file
    manifest = {
        "restart_file": {},
        "namelist_file": {},
        "data_files": []
    }
    for m in matches:
        name = m[1]
        size = m[2]
        tstamp = m[3]
        md5 = m[4]
        if restart_matches and name == restart_matches[0][1]:
            manifest["restart_file"]["name"] = name
            manifest["restart_file"]["size"] = size
            manifest["restart_file"]["timestamp"] = str(tstamp)
            manifest["restart_file"]["md5"] = md5
        elif namelist_matches and name == namelist_matches[0][1]:
            manifest["namelist_file"]["name"] = name
            manifest["namelist_file"]["size"] = size
            manifest["namelist_file"]["timestamp"] = str(tstamp)
            manifest["namelist_file"]["md5"] = md5
        else:
            manifest["data_files"].append({
                "name": name,
                "size": size,
                "timestamp": str(tstamp),
                "md5": md5
            })
    with open("manifest.json", "w+") as f:
        f.write(json.dumps(manifest))

    # Transfer the files downloaded from the zstash archive
    td = globus_sdk.TransferData(tc, source_endpoint, destination_endpoint)

    cwd = os.getcwd()
    source_path = os.path.join(cwd, "manifest.json")
    destination_path = os.path.join(destination_dir, "manifest.json")
    td.add_item(source_path, destination_path)
    for m in matches:
        source_path = os.path.join(cwd, m[1])
        destination_path = os.path.join(destination_dir, m[1])
        td.add_item(source_path, destination_path)

    try:
        task = tc.submit_transfer(td)
        task_id = task.get("task_id")
        logger.info("Submitted Globus transfer: {}".format(task_id))
    except Exception as e:
        logger.error("Globus transfer failed due to error: {}".format(e))
        sys.exit(1)

    """
    A Globus transfer job (task) can be in one of the three states: ACTIVE, SUCCEEDED, FAILED.
    Parsl every 15 seconds polls a status of the transfer job (task) from the Globus Transfer service,
    with 60 second timeout limit. If the task is ACTIVE after time runs out 'task_wait' returns False,
    and True otherwise.
    """
    last_event_time = None
    while not tc.task_wait(task_id, 60, 15):
        task = tc.get_task(task_id)
        # Get the last error Globus event
        events = tc.task_event_list(task_id, num_results=1, filter="is_error:1")
        if not events.data:
            continue
        event = events.data[0]
        # Log the error event if it was not yet logged
        if event["time"] != last_event_time:
            last_event_time = event["time"]
            logger.warn("Non-critical Globus Transfer error event: {} at {}".format(event["description"], event["time"]))
            logger.warn("Globus Transfer error details: {}".format(event["details"]))

    """
    The Globus transfer job (task) has been terminated (is not ACTIVE). Check if the transfer
    SUCCEEDED or FAILED.
    """
    task = tc.get_task(task_id)
    if task["status"] == "SUCCEEDED":
        logger.info("Globus transfer {} succeeded".format(task_id))
    else:
        logger.error("Globus Transfer task: {}".format(task_id))
        events = tc.task_event_list(task_id, num_results=1, filter="is_error:1")
        event = events.data[0]
        logger.error("Globus transfer {} failed due to error: {}".format(task_id, event["details"]))
        sys.exit(1)


if __name__ == "__main__":
    loglevel = os.environ.get("LOGLEVEL", "WARNING").upper()
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=loglevel)

    parser = argparse.ArgumentParser(description="Stage in data files from a zstash archive")
    parser.add_argument("-d", "--destination", required=True,
                        help="destination Globus endpoint and path, <endpoint>:<path>. Endpoint can be a Globus endpoint UUUID or a short name."
                        " Currently recognized short names are: {}.".format(", ".join(name_endpoint.keys())))
    parser.add_argument("-s", "--source",
                        help="source Globus endpoint. If it is not provided, the script tries to determine the source endpoint based on the local hostname.")
    parser.add_argument("-z", "--zstash", required=True,
                        help="zstash archive path")
    parser.add_argument("-c", "--component",
                        help="comma separated components to download (atm, lnd, ice, river, ocean)")
    parser.add_argument("-f", "--pattern-file",
                        help="Pattern file. By default, the patterns are: " + json.dumps(patterns))
    parser.add_argument("-w", "--workers", type=int, default=1,
                        help="Number of workers untarring zstash files")
    parser.add_argument("files", nargs="*",
                        help="List of files to be staged in (standards wildcards supported)")
    args = parser.parse_args()

    main(args)
