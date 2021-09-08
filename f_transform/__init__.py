import io, logging, re
import numpy as np
import pandas as pd

import azure.functions as func

container_input = "reports-ingredion"
container_output = "intermediate-ingredion"

def main(myblob: func.InputStream, outputblob: func.Out[func.InputStream]):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    name = myblob.name
    country_test = re.compile("US|Brazil")
    country = country_test.search(name).group()

    stream = myblob.read()
    #file = io.BytesIO()
    #stream.readinto(file)
    df = pd.read_excel(stream, sheet_name="Data", skiprows=15)
    df = df.iloc[:,6:]
    df = df[df["Company Code"] != "Result"]
    name = f"{country}/"+name[:name.find(country)-1]+".parquet"
    print("Uploading: ", name)
    
    parquet_file = io.BytesIO()
    df.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)
    outputblob.set(parquet_file)
    #outputblob.upload_blob(data=parquet_file)