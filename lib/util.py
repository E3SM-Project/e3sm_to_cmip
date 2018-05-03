import sys
import traceback

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
        print colors.FAIL + '[-] ' + colors.ENDC + colors.BOLD + str(message) + colors.ENDC
    elif status == 'ok':
        print colors.OKGREEN + '[+] ' + colors.ENDC + str(message)
    elif status == 'debug':
        print colors.OKBLUE + '[*] ' + colors.ENDC + str(message) + colors.OKBLUE + ' [*]' + colors.ENDC