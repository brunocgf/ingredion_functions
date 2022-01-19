import io, logging, re, urllib, yaml
import numpy as np
import pandas as pd
import sqlalchemy

import azure.functions as func

# container_input = "raw-data"
# container_output = "clean-data"

def format_file(df):
    df_clean = df.copy()

    #   Change column name
    df_clean.columns.values[2] = 'company_code_id'
    #   Reset index
    df_clean = df_clean.reset_index(drop = True)
    #   Fill all NANs with 0
    df_clean = df_clean.fillna(0)
    #   Remove the "Result" line
    df_clean = df_clean[df_clean['Company Code'] != 'Result']
    #   Split fiscal year and order data frame
    dates = pd.DataFrame(data = df_clean['Fiscal year/period'].astype(str).str.split(pat = '.', expand = True).astype(int).values, columns = ['month', 'year'])
    #  This is to have the correct value for years, have trouble with years that finish in 0 because it has been read as number.
    dates.year[dates.year < 2016] = dates.year[dates.year < 2016]*10
    # month
    df_clean.insert(loc = 1, column = 'month', value = dates.iloc[:, 0])
    # year
    df_clean.insert(loc = 2, column = 'year', value = dates.iloc[:, 1])
    # Sort
    df_clean=df_clean.sort_values(['year', 'month'], ascending=[True, True])
    #df_clean=df_clean.drop(columns=['year', 'month'])
    df_clean['Material'] = df_clean['Material'].astype(str)

    return df_clean

def main(myblob: func.InputStream, outputblob: func.Out[func.InputStream]):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    #name = myblob.name
    #country_test = re.compile("US|CAN|MEX")
    #country = country_test.search(name).group()
    #name = f"{country}/"+name[:name.find(country)-1]+".parquet"

    stream = myblob.read()
    #file = io.BytesIO()
    #stream.readinto(file)
    df = pd.read_excel(stream, 
        sheet_name="Data", 
        skiprows=range(0, 15), 
        usecols = "G:BC")
    #    dtype={'Material': str})
    df = format_file(df)

    with open("credentialsdb.yml") as stream:
        cred = yaml.safe_load(stream)

    params = urllib.parse.quote_plus(
        f"DRIVER={cred['DRIVER']};"
        f"SERVER={cred['SERVER']};"
        f"DATABASE={cred['DATABASE']};"
        f"UID={cred['UID']};"
        f"PWD={cred['PWD']}")

    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    df.to_sql(myblob.name, con=engine, schema='clean', if_exists='append')
    
    parquet_file = io.BytesIO()
    df.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)
    outputblob.set(parquet_file)
    #outputblob.upload_blob(data=parquet_file)