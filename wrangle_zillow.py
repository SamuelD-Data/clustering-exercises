from env import host, user, password

import pandas as pd
import numpy as np
import os

# creates url for connection to data science database
def get_connection(db, user=user, host=host, password=password):
    """
    Function creates a URL that can be used to connect to the data science database.
    """
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

# retrieves data for exercises
def all_2017_zillow_data():
    '''
    This function retrieves data from the zillow codeup data science database and returns it as a dataframe.
    '''
    sql_string = '''
                select * from properties_2017
                join predictions_2017 using (parcelid)
                left join airconditioningtype using (airconditioningtypeid)
                left join architecturalstyletype using (architecturalstyletypeid)
                left join buildingclasstype using (buildingclasstypeid)
                left join heatingorsystemtype using (heatingorsystemtypeid)
                left join propertylandusetype using (propertylandusetypeid)
                left join storytype using (storytypeid)
                left join typeconstructiontype using (typeconstructiontypeid)
                left join unique_properties using (parcelid)
                where latitude is not null and longitude is not null;
                '''
    df = pd.read_sql(sql_string, get_connection('zillow'))
    return df

def missing_rows(df):
    """
    Function creates df which holds each column of passed df as a row and the number of missing rows and percent of missing rows in each of the passed df's rows as columns
    """
    # taking sum of missing rows for each variable, multiplying by 100 then dividing by total 
    # number of rows in original df to find % of missing rows 
    missing_row_percent = df.isnull().sum() * 100 / len(df)
    # count number of missing values for each variable and sum for each
    missing_row_raw = df.isnull().sum()
    # creating df using series' created by 2 previous variables
    missing_df = pd.DataFrame({'num_rows_missing' : missing_row_raw, 'pct_rows_missing': missing_row_percent})
    # return df
    return missing_df

def missing_cols(df):
    """
    Function creates a DF that holds the number of missing columns and percent of missing columns of a passed DF
    """
    # df.isna() displays original df frame with true or false for each value as to whether the value is null
    # .any() creates a series and for each column, shows true if there are any nulls within it (since its being used on the .isna DF)
    # df.loc[ : ] ~~~~~~~~ .count() means we're looking at every row to count the number of null values in each row 
    # using the rest of the code explained above
    num_cols_missing = df.loc[:, df.isna().any()].count()
    # dividing the counts above by the length of the index of the dataframe (ie. the number of rows)
    # to get the percent of missing rows and then rounding to 3 decimal places
    pct_cols_missing = round(num_cols_missing / len(df.index),3)
    # creating dataframe using series' from above
    missing_cols_df = pd.DataFrame({'num_cols_missing': num_cols_missing, 'pct_cols_missing': pct_cols_missing})
    # returning DF
    return missing_cols_df