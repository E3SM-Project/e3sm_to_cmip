
class PS(object):

    def __init__(self, infiles, tables, cmor_input_path, **kwargs):
        self.RAW_VARIABLES = [str('PS')]
        self.VAR_NAME = str('ps')
        self.VAR_UNITS = str('Pa')
        self.TABLE = str('CMIP6_Amon.json')
    
    def write_data(self, varid, data, timeval, timebnds, index):
        """
        No data transform required
        """
        cmor.write(
            varid,
            data[self.RAW_VARIABLES[0]][index, :],
            time_vals=timeval,
            time_bnds=timebnds)
    
    # ------------------------------------------------------------------

class HandlerBase(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.RAW_VARIABLES = []
        self.VAR_NAME = str('')
        self.VAR_UNITS = str('')
        self.TABLE = str('')
    
    def handle(self, infiles, tables, user_input_path, *args, **kwargs):
        """
        Parameters
        ----------
            infiles (List): a list of strings of file names for the raw input data
            tables (str): path to CMOR tables
            user_input_path (str): path to user input json file
        Returns
        -------
            var name (str): the name of the processed variable after processing is complete
        """

        self.handle_variables(
            metadata_path=user_input_path,
            tables=tables,
            table=self.TABLE,
            infiles=infiles,
            raw_variables=self.RAW_VARIABLES,
            write_data=self.write_data,
            outvar_name=self.VAR_NAME,
            outvar_units=self.VAR_UNITS,
            serial=kwargs.get('serial'),
            logging=kwargs.get('logging'))

        return VAR_NAME
    
    def write_data(self):
        print("No handler")
    
    def handle_variables(infiles, raw_variables, write_data, outvar_name, outvar_units, table, tables, metadata_path, serial=None, positive=None, logging=None):
        """
        """
        msg = '{}: Starting'.format(outvar_name)
        if logging:
            logging.info(msg)
        if serial:
            print(msg)

        msg = '{}: running with input files: {}'.format(
            outvar_name,
            infiles)
        logging.info(msg)

        # setup cmor
        setup_cmor(
            outvar_name,
            tables,
            table,
            metadata_path)

        msg = '{}: CMOR setup complete'.format(outvar_name)
        if logging:
            logging.info(msg)
        if serial:
            print(msg)

        data = {}

        # assuming all year ranges are the same for every variable
        num_files_per_variable = len(infiles[raw_variables[0]])

        # sort the input files for each variable
        for var_name in raw_variables:
            infiles[var_name].sort()

        for index in range(num_files_per_variable):

            # reload the dimensions for each time slice
            get_dims = True

            # load data for each variable
            for var_name in raw_variables:

                # extract data from the input file
                msg = '{name}: loading {variable}'.format(
                    name=outvar_name,
                    variable=var_name)
                if logging:
                    logging.info(msg)

                new_data = get_dimension_data(
                    filename=infiles[var_name][index],
                    variable=var_name,
                    levels=False,
                    get_dims=get_dims)
                data.update(new_data)
                get_dims = False

            msg = '{name}: loading axes'.format(name=outvar_name)
            if logging:
                logging.info(msg)
            if serial:
                print(msg)

            # create the cmor variable and axis
            axis_ids, _ = load_axis(data=data)
            if positive:
                varid = cmor.variable(outvar_name, outvar_units,
                                    axis_ids, positive=positive)
            else:
                varid = cmor.variable(outvar_name, outvar_units, axis_ids)

            # write out the data
            msg = "{}: writing {} - {}".format(
                outvar_name,
                data['time_bnds'][0][0],
                data['time_bnds'][-1][-1])
            if logging:
                logging.info(msg)
            if serial:
                print(msg)
                for index, val in enumerate(  # data['time']):
                    progressbar(
                        data['time'],
                        position=0,
                        desc="{}: {} - {}".format(
                            outvar_name,
                            data['time_bnds'][0][0],
                            data['time_bnds'][-1][-1]))):

                    write_data(
                        varid=varid,
                        data=data,
                        timeval=val,
                        timebnds=[data['time_bnds'][index, :]],
                        index=index)
            else:
                for index, val in enumerate(data['time']):
                    write_data(
                        varid=varid,
                        data=data,
                        timeval=val,
                        timebnds=[data['time_bnds'][index, :]],
                        index=index)
        msg = '{}: write complete, closing'.format(outvar_name)
        logging.info(msg)
        if serial:
            print(msg)
        cmor.close()
        msg = '{}: file close complete'.format(outvar_name)
        logging.info(msg)
        if serial:
            print(msg)