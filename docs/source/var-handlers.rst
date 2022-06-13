Variable Handlers
-----------------

What are variable handlers?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ``e3sm_to_cmip`` package, each supported CMIP6 variable has a “handler”. This handler defines the metadata necessary for CMORizing E3SM variable(s) to the equivalent CMIP6 variable.

Variable handler definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The metadata for variable handlers are defined in (key, value) pairs.

.. note::
    Required keys:

    - name: The CMIP6 variable name.
    - units: The CMIP6 variable units.
    - raw_variables: The E3SM variable name(s) used in the conversion to the CMIP6 variable.
    - table: The default CMOR table filename. (Source: https://github.com/PCMDI/cmip6-cmor-tables/Tables)

    Optional keys (based on variable):

    - unit_conversion: An optional unit conversion formula for the final output data.
    - formula: An optional conversion formula for calculating the final output data. Usually this is defined if there are more than one raw variable.
    - positive: The "positive" directive to CMOR enables data providers to specify the direction that they have assumed in fields  (i.g. radiation fluxes has up or down direction) passed to CMOR. If their direction is opposite that required by CMIP6 (as specified in the CMOR tables), then CMOR will multiply the field by -1, reversing the sign for consistency with the data request.
    - levels: Distinguishes model-level variables, which require remapping from the default model  level to the level defined in the levels dictionary.

Where are variable handlers defined?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The supported CMIP6 variables and their handlers are defined in the `E3SM CMIP6 Data Conversion Tables Confluence page`_.

.. _E3SM CMIP6 Data Conversion Tables Confluence page: https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/858882132/CMIP6+data+conversion+tables

These handler definitions are transferred to the ``e3sm_to_cmip`` repository at the following locations:

1. ``e3sm_to_cmip/resources/handlers.yaml``

    - **Stores handler definitions for atmosphere and land variables.**
    - Each top-level entry defines a handler.
    - There can be more than one handler per variable (e.g., to handle different frequencies)

    - Example:

    .. code-block:: yaml

        -   name: pr
            units: kg m-2 s-1
            raw_variables: [PRECT]
            table: CMIP6_day.json
            unit_conversion: null
            formula: PRECT * 1000.0
            positive: null
            levels: null
        -   name: pr
            units: kg m-2 s-1
            raw_variables: [PRECC, PRECL]
            table: CMIP6_Amon.json
            unit_conversion: null
            formula: (PRECC + PRECL) * 1000.0
            positive: null
            levels: null

2. ``e3sm_to_cmip/cmor_handlers`` directory
    - Each handler is defined as Python module (``.py`` file). This is the legacy design for defining handlers, which is progressively being be replaced by ``handlers.yaml``.
    - ``/mpas_vars`` **stores handler definitions for MPAS ocean and sea-ice variable handlers.** MPAS variables require additional processing requirements (e.g., use of mesh files). The development team is considering refactoring the design of these handlers.
    - ``/vars`` **stores legacy atmosphere land variable handlers** (``areacella``, ``clisccp``, ``orog``, ``pfull``, ``phalf``). These handlers need to be refactored since they still depend on CDAT modules or contain redudant code that has since been generalized into helper functions. **DO NOT add any new handlers in this directory. Instead, use handlers.yaml.**

How to add new atmosphere and land variable handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Append a new entry to the ``e3sm_to_cmip/resources/handlers.yaml`` file.
2. If the handler has a ``formula``, add a formula function to ``e3sm_to_cmip/cmor_handlers/_formulas.py``.

  - The function parameters must include ``data`` (a dictionary mapping variables to its underlying ``np.ndarray``) and ``index`` (the index within the array to apply the formula to).
  - Example:

    .. code-block:: python

        def cLitter(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
            """
            cLitter = (TOTLITC + CWDC)/1000.0
            """
            outdata = (data["TOTLITC"][index, :] + data["CWDC"][index, :]) / 1000.0

            return outdata

How to add new MPAS ocean and sea-ice variable handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Adding a variable handler for MPAS variables is slightly more involved process.

You need to create a Python module in ``/cmor_handlers/mpas_vars``. We recommend taking a look
at the existing modules such as ``so.py`` to get idea on how to add an MPAS handler.

How ``e3sm_to_cmip`` derives handlers for variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``e3sm_to_cmip`` derives the appropriate variable handlers to use based on the available E3SM variables in the input datasets. Afterwards, it applies any necessary unit conversions, formulas, etc. during the CMORizng process.

For example, let's say we want to CMORize the variable ``"pr"`` and we pass an E3SM input dataset that has the variables ``"PRECC"`` and ``"PRECL"``. ``e3sm_to_cmip`` applies this logic to derive the appropriate ``"pr"`` variable handler:

1. Run ``e3sm_to_cmip --var-list pr --input-path <SOME_INPUT_PATH>``
2. ``--var-list`` is stored in a list, ``var_list=["pr"]``.
3. All defined handlers are gathered in a dictionary called ``available_handlers``:

    .. code-block:: python

        # Key = CMIP variable id,  value = list of available handler objects defined in ``handlers.yaml`` and `/cmor_handlers`
        available_handlers = {
                "pr": [
                    VarHandler(name="pr", raw_variables=["PRECT"]),
                    VarHandler(name="pr", raw_variables=["PRECC", "PRECL"]),
                ],
            }

4. Loop over ``var_list``:

    a. Get the list of handlers from ``available_handlers`` dict (for ``"pr"``)

        .. code-block:: python

            [
                VarHandler(name="pr", raw_variables=["PRECT"]),
                VarHandler(name="pr", raw_variables=["PRECC", "PRECL"]),
            ]

    b. Derive a handler using the variables in the E3SM input dataset

        - The E3SM input dataset contains ``"PRECC"`` and ``"PRECL"``, so we derive the second handler, ``VarHandler(name="pr", raw_variables=["PRECC", "PRECL"])``.
        - If no handler can be derived, an error is raised.

    c. Append derived handler to final list of ``derived_handlers``

5. Return ``derived_handlers=[VarHandler(name="pr", raw_variables=["PRECC", "PRECL"])]``
