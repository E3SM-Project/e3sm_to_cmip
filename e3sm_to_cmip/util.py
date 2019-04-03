from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import traceback
import cdutil
import cmor
import os

def format_debug(e):
    """
    Return a string of an exceptions relavent information
    """
    _, _, tb = sys.exc_info()
    return """
1: {doc}
2: {exec_info}
3: {exec_0}
4: {exec_1}
5: {lineno}
6: {stack}
""".format(
    doc=e.__doc__,
    exec_info=sys.exc_info(),
    exec_0=sys.exc_info()[0],
    exec_1=sys.exc_info()[1],
    lineno=traceback.tb_lineno(sys.exc_info()[2]),
    stack=traceback.print_tb(tb))

class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_message(message, status='error'):
    """
    Prints a message with either a green + or a red -

    Parameters:
        message (str): the message to print
        status (str): th"""
    if status == 'error':
        print(colors.FAIL + '[-] ' + colors.ENDC + colors.BOLD + str(message)
              + colors.ENDC)
    elif status == 'ok':
        print(colors.OKGREEN + '[+] ' + colors.ENDC + str(message))
    elif status == 'debug':
        print(colors.OKBLUE + '[*] ' + colors.ENDC + str(message)
              + colors.OKBLUE + ' [*]' + colors.ENDC)


def hybrid_to_plevs(var, hyam, hybm, ps, plev):
    """Convert from hybrid pressure coordinate to desired pressure level(s).

    Parameters
    ----------
        var (cdms2 transient variable): the variable to convert into the new plev
        hyam (cdms2 transient variable): the hyam attribute from the file that contained the variable
        hybm (cdms2 transient variable): the hybm attribute
        ps (cdms2 transient variable): the PS variable from the file
        plev (list): A list of integers containing the new pressure levels

    Returns
    -------
        var_p (cdms2 transient variable): the var variable, but converted to the new plev levels

    Notes:
        This is taken from the e3sm_diags package. Original code can be found here https://github.com/E3SM-Project/e3sm_diags/blob/master/acme_diags/driver/utils/general.py#L107
    """
    p0 = 1000.  # mb
    ps = ps / 100.  # convert unit from 'Pa' to mb
    levels_orig = cdutil.vertical.reconstructPressureFromHybrid(
        ps, hyam, hybm, p0)
    levels_orig.units = 'mb'
    # Make sure z is positive down
    if var.getLevel()[0] > var.getLevel()[-1]:
        var = var(lev=slice(-1, None, -1))
        levels_orig = levels_orig(lev=slice(-1, None, -1))
    var_p = cdutil.vertical.logLinearInterpolation(
        var(squeeze=1),
        levels_orig(squeeze=1), plev)

    return var_p

def setup_cmor(var_name, table_path, table_name, user_input_path):
    
    var_name = var_name.encode('ascii')
    table_path = table_path.encode('ascii')
    table_name = table_name.encode('ascii')
    user_input_path = user_input_path.encode('ascii')

    logfile = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logfile):
        os.makedirs(logfile)

    logfile = os.path.join(logfile, var_name + '.log')
    cmor.setup(
        inpath=table_path,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)

    cmor.dataset_json(user_input_path)
    try:
        cmor.load_table(table_name)
    except (Exception, BaseException) as error:
        print(format_debug(error))
