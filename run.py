import logging 
import os 
import sys 

import uvicorn 

if os .path .exists (".env"):
    with open (".env","r")as f :
        for line in f :
            line =line .strip ()
            if line and not line .startswith ("#")and "="in line :
                key ,val =line .split ("=",1 )
                os .environ [key ]=val 


def setup_logging ():
    """Configure file + console logging for the entire application."""
    log =logging .getLogger ("leads_importer")
    log .setLevel (logging .DEBUG )

    fmt =logging .Formatter (
    "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt ="%Y-%m-%d %H:%M:%S",
    )

    fh =logging .FileHandler ("import.log",encoding ="utf-8")
    fh .setLevel (logging .INFO )
    fh .setFormatter (fmt )
    log .addHandler (fh )

    ch =logging .StreamHandler (sys .stdout )
    ch .setLevel (logging .INFO )
    ch .setFormatter (fmt )
    log .addHandler (ch )


if __name__ =="__main__":
    setup_logging ()
    logging .getLogger ("leads_importer").info ("Starting API server")
    uvicorn .run ("src.api:app",host ="0.0.0.0",port =8000 ,reload =True )
