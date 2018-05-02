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