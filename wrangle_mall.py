# imports
from env import host, user, password

import pandas as pd
import numpy as np
import os
import sklearn

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler

# creates sql string for connection to data science database
def get_connection(db, user=user, host=host, password=password):
    """
    Function creates a URL that can be used to connect to the data science database.
    """
    # return string to access database
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

# function to acquire mall data
def new_mall_data():
    '''This function reads the mall customer data from the Codeup db into a df, writes it to a csv file and returns the df'''
    sql_query = 'SELECT * FROM customers'
    df = pd.read_sql(sql_query, get_connection('mall_customers'))
    df.to_csv('mall_customers_df.csv')
    return df

# function to detect upper outliers
def get_upper_outliers(s, k):
    '''
    Function takes in a series (s) and cuttoff value (k). 
    If a value in the series is an outlier, it returns a number that represents how far above the value is from the upper bound
    or 0 if the number is not an outlier.
    '''
    # creating 2 variables that represent the 1st and 3rd quantile of the given series
    q1, q3 = s.quantile([.25, .75])
    # calculating IQR
    iqr = q3 - q1
    # calculating upper bound
    upper_bound = q3 + k * iqr
    # returning series described in doc string
    return s.apply(lambda x: max([x - upper_bound, 0]))

# function to split data into train, validate, test samples
def prep_mall_data(df, target):
    """
    This function accepts a dataframe and returns it split into 3 appropriately proportioned DFs for training, validating, and testing purposes.
    """
    # splitting data
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_validate, test_size=.3, random_state=123)
    
    # specifying which columns to keep in outputted dataframe
    # x = features | y = target variable
    X_train = train.drop(columns=[target])
    y_train = train[[target]]
    
    X_validate = validate.drop(columns=[target])
    y_validate = validate[[target]]
    
    X_test = test.drop(columns=[target])
    y_test = test[[target]]
    
    return X_train, y_train, X_validate, y_validate, X_test, y_test

# function to encode gender as binary values (0 and 1)
def encode_gender(df):
    """
    Function accepts a DF with a column named "gender", and returns the dataframe with a new column that is an encoded version of the gender column.
    1 = Male | 0 = Female
    """
    # creating label encoder object
    label_encoder = LabelEncoder()
    # fitting object to gender column
    gender_encoded = label_encoder.fit_transform(df.gender)
    # adding column "male" to passed df and storing encoded gender values in it
    df['male'] = gender_encoded
    # returning df
    return df

# function to remove rows and columns based amount of missing values
def handle_missing_values(df, prop_required_column = .4, prop_required_row = .6):
    '''
    Function accepts 3 values: dataframe, and two separate values from 0 - 1
    The passed dataframe will have rows and columns removed based on the amount of null values in each
    The first numeric value passed specifies what % of each columns values must be non-null to avoid being dropped
    The second numeric value passed specifies what % of each row values must be non-null to avoid being dropped
    '''
    # dropping columns based on % of missing values
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    # dropping rows based on % of missing values
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    # returning updated df
    return df

# function to scale mall data
def mall_scaler(X_train, X_validate, X_test):
    """
    Function accepts 3 mall dataframes output by prep_mall_data function and returns them with the age and annual income columns scaled.
    Spending score is not scaled because it is our target variable.
    """
    # creating scaler object
    scaler = sklearn.preprocessing.MinMaxScaler()
    
    # fitting scaler to x train 
    scaler.fit(X_train[['age', 'annual_income']])

    # scaling data and saving to new dataframes
    X_train_scaled = pd.DataFrame(scaler.transform(X_train[['age', 'annual_income']]))
    X_validate_scaled = pd.DataFrame(scaler.transform(X_validate[['age', 'annual_income']]))
    X_test_scaled = pd.DataFrame(scaler.transform(X_test[['age', 'annual_income']]))
    
    # renaming columns in new dataframes as they were given integers as names
    X_train_scaled.rename(columns = {0: 'age', 1: 'annual_income'}, inplace=True)
    X_validate_scaled.rename(columns = {0: 'age', 1: 'annual_income'}, inplace=True)
    X_test_scaled.rename(columns = {0: 'age', 1: 'annual_income'}, inplace=True)
    
    # returning new scaled dataframes
    return X_train_scaled, X_validate_scaled, X_test_scaled