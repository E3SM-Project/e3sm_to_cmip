Development Guide
=================

Testing changes and debugging in Python
---------------------------------------

In older versions of ``e3sm_to_cmip``, the only way to test changes was to run ``e3sm_to_cmip`` directly on the command line. This debugging process often involves adding ``print`` or ``ipdb`` statements throughout the codebase, which generally isn't good practice because it is inefficient and developers might forget to delete those statements.

As of ``e3sm_to_cmip > 1.91``,  ``e3sm_to_cmip`` can now be executed through a Python script. Advantages of using this approach include:

- Testing and debugging changes are significantly more efficient, which shortens the debugging cycle.
- Leverage IDEs to set breakpoints in IDEs and step through the call stack at runtime.
- Gain a better sense of how functions are manipulating variables and whether the correct behaviors are being produced.
- Prototype code in the debugger and implement those changes, then test if it behaves as expected.

Example 1 (CMORizing serially)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CLI Execution

.. code-block:: bash

    e3sm_to_cmip --output-path ../qa/tmp --var-list 'pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, hur' --input-path /lcrc/group/e3sm/e3sm_to_cmip/input/atm-unified-eam-ncclimo --user-metadata /home/ac.tvo/E3SM-Project/CMIP6-Metadata/template.json --tables-path /home/ac.tvo/PCMDI/cmip6-cmor-tables/Tables/ --serial

Python Execution

.. code-block:: python

    from e3sm_to_cmip.__main__ import main

    args = [
        "--var-list",
        'pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, hur',
        "--input",
        "/lcrc/group/e3sm/e3sm_to_cmip/input/atm-unified-eam-ncclimo",
        "--output",
        "../qa/tmp",
        "--tables-path",
        "/lcrc/group/e3sm/e3sm_to_cmip/cmip6-cmor-tables/Tables/",
        "--user-metadata",
        "/lcrc/group/e3sm/e3sm_to_cmip/template.json",
        "--serial"
    ]

    # `main()` creates an `E3SMtoCMIP` object and passes `args` to it, which sets the object parameters to execute a run.
    main(args)

Example 2 (info mode)
~~~~~~~~~~~~~~~~~~~~~

CLI Execution

.. code-block:: bash

    e3sm_to_cmip --info -v prw, pr --input /p/user_pub/work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/atmos/native/model-output/day/ens1/v1/ --tables /home/vo13/PCMDI/cmip6-cmor-tables/Tables/

Python Execution

.. code-block:: python

    from e3sm_to_cmip.__main__ import main

    args = [
        "--info",
        "-v",
        "prw, pr",
        "--input",
        "/p/user_pub/work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/atmos/native/model-output/day/ens1/v1/",
        "--output",
        "../qa/tmp",
        "--tables-path",
        "/home/vo13/PCMDI/cmip6-cmor-tables/Tables/",
    ]

    # `main()` creates an `E3SMtoCMIP` object and passes `args` to it, which sets object parameters to execute a run.
    main(args)


Example 3 (E3SMtoCMIP class inspection)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This process is useful for checking how ``e3sm_to_cmip`` interprets the CLI arguments, and which handlers are derived based on ``--var-list``.


.. code-block:: python

    from e3sm_to_cmip.__main__ import E3SMtoCMIP

    args = [
        "--var-list",
        'pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, hur',
        "--input",
        "/lcrc/group/e3sm/e3sm_to_cmip/input/atm-unified-eam-ncclimo",
        "--output",
        "../qa/tmp",
        "--tables-path",
        "/lcrc/group/e3sm/e3sm_to_cmip/cmip6-cmor-tables/Tables/",
        "--user-metadata",
        "/lcrc/group/e3sm/e3sm_to_cmip/template.json",
        "--serial"
    ]

    run = E3SMtoCMIP(args)

    # Now we can check the `E3SMtoCMIP` object attributes for the `run` variable.
    print(run.handlers)
