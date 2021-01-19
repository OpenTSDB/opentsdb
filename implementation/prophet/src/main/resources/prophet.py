# This file is part of OpenTSDB.
# Copyright (C) 2021  The OpenTSDB Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import os, time

# Set the zone to UTC globaly hopefully?
#os.environ['TZ'] = 'UTC'
#time.tzset()
from dateutil.tz import *
from datetime import *
import pytz
print("TZ: " + datetime.now(tzlocal()).tzname())

import pandas as pd
from fbprophet import Prophet
import json
from fbprophet.serialize import model_to_json, model_from_json

# Parse the settings. We're passing in JSON over stdin.
incoming_data = sys.stdin.readline()
print("Incoming Data:" + incoming_data)
settings = json.loads(sys.stdin.readline())

# Parse the json into a Pandas data frame
df = pd.read_json(path_or_buf=incoming_data, orient='records')

# Setup the prophet object.
m = Prophet(
    growth = settings['growth'].lower(),
    n_changepoints = settings['numberOfChangePoints'],
    changepoint_range = settings['changePointRange'],
    yearly_seasonality = settings['yearlySeasonality'] if 'yearlySeasonality' in settings else 'auto',
    weekly_seasonality = settings['weeklySeasonality'] if 'weeklySeasonality' in settings else 'auto',
    daily_seasonality = settings['dailySeasonality'] if 'dailySeasonality' in settings else 'auto',
    seasonality_mode = settings['seasonality'].lower(),
    seasonality_prior_scale = settings['seasonalityPriorScale'],
    holidays_prior_scale = settings['holidaysPriorScale'],
    changepoint_prior_scale = settings['changePointPriorScale'],
    mcmc_samples = settings['mcmcSamples'],
    interval_width = settings['uncertaintyIntervalWidth'],
    uncertainty_samples = settings['uncertaintySamples']
)

# Fit it. Got to do it.
m.fit(df)

# Something?
future = m.make_future_dataframe(periods=60 if settings['modelInterval'] == "h" else 1440, freq='min', include_history=False)
forecast = m.predict(future)
#forecast.tz_localize('UTC')

#forecast.index=pd.DatetimeIndex(forecast[['ds']]).tz_localize('UTC')
#print(forecast.index)
forecast['ds'] = forecast['ds'].dt.tz_localize(tzlocal())
print(forecast)

# Done, now output the data as JSON to parse back into Java.
print("Forecast: " + forecast[['ds', 'yhat']].to_json(date_unit='s', date_format='epoch'))
print("Model: " + model_to_json(m))

