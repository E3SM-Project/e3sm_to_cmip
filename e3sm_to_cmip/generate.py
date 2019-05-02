import os
import jinja2
import sys
import yaml


def generate(defaults_path, handler_path):

    with open(defaults_path, 'r') as infile:
        defaults = yaml.load(infile, Loader=yaml.FullLoader)
        template_path = os.path.join(handler_path, 'default.template')
        for default in defaults['default_handlers']:
            variables = {
                'name': default.get('cmip_name'),
                'raw_var': default.get('e3sm_name'),
                'units': default.get('units'),
                'table': default.get('table'),
                'positive': "str(\'{}\')".format(default['positive']) if default.get('positive') else 'None'
            }
            out_path = os.path.join(handler_path, variables['name'] + '.py')
            if os.path.exists(out_path):
                os.remove(out_path)
            render(
                variables,
                template_path,
                out_path)
    return 0


def render(variables, input_path, output_path):
    """
    Renders the jinja2 template from the input_path into the output_path
    using the variables from variables
    """

    tail, head = os.path.split(input_path)
    template_path = os.path.abspath(tail)

    loader = jinja2.FileSystemLoader(searchpath=template_path)
    env = jinja2.Environment(loader=loader)

    template = env.get_template(head)
    outstr = template.render(variables)

    with open(output_path, 'a+') as outfile:
        outfile.write(outstr)


if __name__ == "__main__":
    sys.exit(generate(sys.argv[1], sys.argv[2]))
