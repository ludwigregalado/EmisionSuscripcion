# -*- coding: utf-8 -*-
"""
Created on Fri Apr  9 08:52:52 2021

@author: Jorge ludwig Regalado de la Rosa
@position: Process Intelligence Enginneer
@company: HDI, Seguros
"""
import pandas as pd # Dataframe manipulation
import pyodbc # Database connection
import pymc3 as pm
from datetime import timedelta, datetime # Time and date manipulation
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.filtering.log.start_activities import start_activities_filter
from pm4py.algo.filtering.log.end_activities import end_activities_filter

def importing_data(file):
        
    # Reading database connection information from files
    server_file = open('server.sql','r')
    database_file = open('database.sql', 'r')
    
    # Reading file which contains a long query
    with open(file, encoding='utf-8') as inserts:
        query = inserts.read()

    # Settings to connect with the database
    server = server_file.readline()
    database = database_file.readline()
    
    # Closing file objects
    server_file.close()
    database_file.close()
    
    # Connecting to database
    DWH = pyodbc.connect(r'DRIVER={SQL Server};SERVER='+server+
                         ';DATABASE='+database+
                         ';TRUSTED_CONNECTION=TRUE')
    # Query from DWH
    datos = pd.read_sql_query(query, DWH)
    
    return(datos)

def process_filter(dataframe):
    """
    Filter OTs that started with Pendiente/Rechazada and Finish with Emitida
    using pm4py capabilities
    """
    
    # This line assures that date column is well formatted
    log_df =  dataframe_utils.convert_timestamp_columns_in_df(dataframe)
    
    # Renaming columns to use pm4py smoothly. ***IMPROVE IT***
    log_df.rename(columns = {'IdOT':'case:concept:name',
                             'TiempoAlta':'time:timestamp',
                             'Resource': 'org:resource',
                             'IdStatus': 'concept:name'}, inplace = True)
    # parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: caseId}# Indicates which column is case Id
    
    # Sorting dataframe entries by date
    log_df = log_df.sort_values('time:timestamp')
    
    # Converting dataframe to event log
    event_log = log_converter.apply(log_df)
    # event_log = log_converter.apply(log_df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)# From df to log
    
    
    # Dicovering the start and end activities
    # start_activities = start_activities_filter.get_start_activities(event_log)
    # end_activities = end_activities_filter.get_end_activities(event_log)
    
    # Filtering data
    filtered_log = start_activities_filter.apply(event_log, ["Pendiente",
                                                             "Rechazada"])
    filtered_log = end_activities_filter.apply(filtered_log, ["Emitida",
                                                             "Rechazada"])
    
    # Converting log to dataframe
    df = log_converter.apply(filtered_log, variant=log_converter.Variants.TO_DATA_FRAME)
    df.rename(columns = {'case:concept:name': 'IdOT',
                         'time:timestamp': 'TiempoAlta',
                         'org:resource': 'Resource',
                         'concept:name': 'Status'}, inplace = True)

    # Grouping by Id to calculate total elapsed time
    df = df.groupby(['IdOT'], as_index = False).agg([max, min])
    df['TiempoAtencion'] = round((df['TiempoAlta']['max'] - df['TiempoAlta']['min']).dt.total_seconds()/3600)

    return(df)

def extract_lims(datos):# Extract the first and last update for each OT
    maximos = datos.loc[:,'TiempoAlta']['max']
    minimos = datos.loc[:,'TiempoAlta']['min']
    
    return(minimos, maximos)

def MCMC(datosTiempo):
    # Generating Model context for tiempoEmision
    with pm.Model() as model:
        # Emisión
        alpha = 1.0/(datosTiempo).mean()
        lambda_1 = pm.Exponential("lambda_1", alpha)

    # Generating lambda
    with model:
        lambda_ = lambda_1

    # Generating observation distributions
    with model:
        observation = pm.Poisson("obs", lambda_, observed = datosTiempo)

    # Generating simulations
    with model:
        step = pm.Metropolis()
        trace = pm.sample(1000, tune = 500, step = step, progressbar = True)

#     lambda_1_samples= trace['lambda_1']
    
    return(model, trace)