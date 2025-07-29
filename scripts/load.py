def run():
    print('load started ///HEREE')
    import sys

    # Print the Python path
    print('syspath;')
    print(sys.path)
    
    import asyncio
    import aiohttp
    import json
    import os
    from datetime import timedelta, datetime
    import sqlite3
    import great_expectations as gx
    context = gx.get_context()
    print(gx.__version__)
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")

    import pandas as pd
    print("-- LOAD running -- ")
