from sys import exit, argv
import jinja2 import Template
import argparse

template_string = """#!/bin/bash
#!/bin/bash
# ---------------------------------------------------------------------
#SBATCH --account={account}
#SBATCH --time=1-00:00
# ---------------------------------------------------------------------

RETURN=true
until [ RETURN -eq 0 ]; do
    
    RETURN=/qfs/people/bald158/anaconda2/envs/cwl/bin/e3sm_to_cmip \
        --no-metadata -s --mode ocn 
        -v {variables} \
        --tables-path {tables} \
        --user-metadata {metadata} \
        --num-proc {num_proc} \
        --map {map} \
        --logdir {outdir} \
        --output {outdir} \
        --input {input} \
        --timeout {timeout}
done
"""

def render_sbatch(values):
    template = Template(template_string)
    try:
        script_contents = template.render(
            variables=values.variables,
            name=values.name,
            start=values.start,
            end=values.end,
            account=values.account,
            tables=values.tables,
            metadata=values.metadata,
            num_proc=values.stnum_procart,
            map=values.map,
            outdir=values.outdir,
            input=values.input)
        with open('run_mpaso_sbatch.sh', 'w') as outfile:
            outfile.write(script_contents)
    except Exception as e:
        raise(e)
        return 1
    else:
        return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--variables')
    parser.add_argument('--account')
    parser.add_argument('--tables')
    parser.add_argument('--metadata')
    parser.add_argument('--num_proc')
    parser.add_argument('--map')
    parser.add_argument('--outdir')
    parser.add_argument('--input')
    parser.add_argument('--timeout')
    exit(
        render_sbatch(
            parser.parse_args()))
