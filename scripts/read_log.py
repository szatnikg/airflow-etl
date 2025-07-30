import sys
import os


sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))
from static_dir import StaticDirectory

def run():
    sd = StaticDirectory()
    with open(sd.logpath,'r',encoding='utf-8') as lf:
        print(lf.read())
        print('\n ==== LOG END ==== ')

if __name__=="__main__":
    run()