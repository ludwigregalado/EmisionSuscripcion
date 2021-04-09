# -*- coding: utf-8 -*-
"""
Estimating request order waiting times based on finished ones.

@author: Jorge ludwig Regalado de la Rosa, M. Sc
@position: Process Intelligence Enginneer
@company: HDI, Seguros
"""

# Importing all the necessary libraries
# import matplotlib.pyplot as plt # Plotting
# import seaborn as sns # Fancy visualization capabilities

import numpy as np # Numerical array manipulation
import pymc3 as pm

import theano.tensor as tt
import functionES as es

#  Loading data
datos = es.importing_data('OTs_detallado.sql')

# Filtering data to keep the request orders finished
datosObservados = es.process_filter(datos)# Advanced process mining filtering

# ----------------------------------------------------------------------------
# **************************Monte Carlo Markov Chain**************************
# ----------------------------------------------------------------------------

count_obs = len(datosObservados)# How many observations we have

# Generating Model context
with pm.Model() as model:
    alpha = 1.0/(datosObservados.TiempoAtencion).mean()
    
    lambda_1 = pm.Exponential("lambda_1", alpha)
    lambda_2 = pm.Exponential("lambda_2", alpha)

    tau = pm.DiscreteUniform("tau", lower=0, upper=count_obs-1)

# Generating lambdas (two because at some point, lambda changes its value)
with model:
    idx = np.arange(count_obs)
    lambda_ = pm.math.switch(tau > idx, lambda_1, lambda_2)
    
# Generation Observation distribution
with model:
    observation = pm.Poisson("obs", lambda_, observed = datosObservados.TiempoAtencion)
    
# Generating simulations
with model:
    step = pm.Metropolis()
    trace = pm.sample(100, tune = 50, step = step, progressbar = True)
    
lambda_1_samples = trace['lambda_1']
lambda_2_samples = trace['lambda_2']
tau_samples = trace['tau']