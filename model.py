import sqlite3
import sys
import pandas as pd
import numpy as np
#initial parameters
db = sqlite3.connect(sys.argv[1])
w_station = sys.argv[2]
l_zone = sys.argv[3]
pd.options.mode.chained_assignment = None
'''
imports the relevant weather data and the regional power load
from the database given as a parameter
'''
def import_rel_data():
    tempf = w_station + ".TemperatureF"
    roundutc = w_station + ".RoundedUTC"
    query = "SELECT OperDay, HourEnding, " + tempf + " as TemperatureF, " + l_zone + " FROM zonalload INNER JOIN " + w_station + " ON " + roundutc + " = zonalload.DateUTC"
    return pd.read_sql_query(query,db)

'''
normalizes the power load and temperature
'''
def normalize_data(data):
    datmin = data.min()
    datmax = data.max()
    data = (data - datmin)/(datmax-datmin)
    return (data, datmin, datmax)

'''
modifies the temperature dataframe to replace it with the difference between
the test temperature and the baseline temperature with the lowest peak power
zonal_load
'''
def modify_temp(data):
    temp = data.groupby(data['OperDay']).max()['TemperatureF']
    optemp = data.groupby(data['OperDay']).max().min()[1]
    temp_mod = abs(temp - optemp)
    return temp_mod
'''
obtains the temperature in each day that is closest to the baseline temperature in
the model data, and is subtracted from the test data to create a temperature deta
column
'''
def get_temp_close_to_op_temp(test_data,model_data):
    model_data = model_data.groupby(data['OperDay'])
    temp = model_data.max().min()[2]
    test_data['TemperatureF'] = (test_data['TemperatureF']-temp).abs()
    dataparsed = test_data.loc[test_data.groupby('OperDay')['TemperatureF'].idxmin()].reset_index()
    return dataparsed.filter(['OperDay','TemperatureF'])

'''
linear regression on the normalized temperature delta column and the normalized
peak power load; returns coefficients of regression equation
'''
def get_poly(data):
    x = data['TemperatureF'].tolist()
    y = data[l_zone].tolist()
    coefficients = np.polynomial.polynomial.polyfit(x, y, 1)
    return coefficients

'''
returns the predicted power load from running the regression equation on the
normalized temperature delta data to the predicted linear regression polynomial,
takes in the max and min values from earlier to unnormalized the peak loads
'''
def apply_temp(data, poly,l_min,l_max):
    data[l_zone] = (np.polynomial.polynomial.polyval(data['TemperatureF'], poly))
    data[l_zone] = data[l_zone] * (l_max - l_min) + l_min
    return data.filter(['OperDay', l_zone])

#combines the temperature and normalized load temperature dataframes
def join_temp_with_load(temp,load):
    return temp.to_frame().join(load)
'''
main model predicting logic: takes in the model_data to generate the regression
equation; the test_data is then inputted to create a prediction
'''
def generate_predicted_loads(model_data,test_data):

    #finds the optimal temperature of each day to input to the regression equation
    modified_test_data = get_temp_close_to_op_temp(test_data,model_data)
    #normalization of data, returns min and max for denormalization later
    normalized_load_data = normalize_data(model_data.groupby(data['OperDay']).max()[l_zone])
    #access the various properties of the tuple returned
    normalized_load_frame = normalized_load_data[0]
    load_min = normalized_load_data[1]
    load_max = normalized_load_data[2]
    #combine the normalized data frame with the temperature delta column
    modified_frame = join_temp_with_load(modify_temp(model_data), normalized_load_frame)
    #regression analysis on model data, which then the test data is inputted to
    model_prediction = (apply_temp(modified_test_data,get_poly(modified_frame),load_min,load_max))
    model_prediction = model_prediction.rename(columns={'OperDay':'Date', l_zone : 'Forecased_Peak_Load'})
    #predicted data being inserted into a csv file
    model_prediction.to_csv('model.csv', index = False)
    print("Data importation into csv complete!")


data = import_rel_data()
model_data = data[data.OperDay.str.endswith('2014')]
test_data = data[data.OperDay.str.endswith('2015')]
generate_predicted_loads(model_data,test_data)
