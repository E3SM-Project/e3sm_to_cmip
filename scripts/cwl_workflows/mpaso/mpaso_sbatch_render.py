from sys import exit, argv
from subprocess import call
from jinja2 import Template
import argparse

template_string = """#!/bin/bash

RETURN=1
until [ $RETURN -eq 0 ]; do
    
    /qfs/people/bald158/anaconda2/envs/cwl/bin/e3sm_to_cmip \
        --no-metadata --mode mpaso --precheck --no-rm-tmpdir \
        -v {{ variables }} \
        --tables-path {{ tables }} \
        --user-metadata {{ metadata }} \
        --num-proc {{ num_proc }} \
        --map {{ map }} \
        --logdir {{ outdir }} \
        --output {{ outdir }} \
        --input {{ input }} \
        --timeout {{ timeout }}
    RETURN=$?
    if [ $RETURN != 0 ]; then
        echo "Restarting"
    fi
done
"""

def render_sbatch(values):
    template = Template(template_string)
    script_path = 'run_mpaso_sbatch.sh'
    try:
        script_contents = template.render(
            variables=values.variables,
            account=values.account,
            tables=values.tables,
            metadata=values.metadata,
            num_proc=values.num_proc,
            map=values.map,
            outdir=values.outdir,
            input=values.input,
            timeout=values.timeout)
        with open(script_path, 'w') as outfile:
            outfile.write(script_contents)
        
        call(['chmod', '+x', script_path])
        call(['srun', '-A', 'e3sm', '-t', '1-00:00', script_path])
    except Exception as e:
        raise(e)
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
