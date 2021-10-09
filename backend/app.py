from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
from fastapi import BackgroundTasks, FastAPI
from snps import SNPs
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from starlette.responses import Response
import os

from urllib.request import urlretrieve

GWAS_URL = "https://www.ebi.ac.uk/gwas/api/search/downloads/alternative"
GWAS_FILEPATH = f"gwas_{datetime.today().strftime('%d%m%Y')}.tsv"
GWAS_TABLE = "gwas"
DB_URL = "sqlite:///main.db"

app = FastAPI()
origins = ["null"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root():
    return Response("We are running", status_code=200)


@app.get("/gwas")
async def gwas_endpoint(bt: BackgroundTasks):
    bt.add_task(urlretrieve, url=GWAS_URL, filename=GWAS_FILEPATH)
    return Response("Triggered", status_code=200)


@app.get("/db")
async def create_db(bt: BackgroundTasks):
    bt.add_task(db_setup)
    return Response("Triggered", status_code=200)


@app.post("/candy/{file_id}")
async def process_file(bt: BackgroundTasks, file_id: int):

    # Trigger background task to run the party
    print("hey yeeey")
    bt.add_task(magic, file_id=file_id)

    return Response("Started", status_code=202)


def db_setup():
    # create db if not exist on url
    db: Engine = create_engine(DB_URL)

    # load gwas if not exists
    # These two should be independent. DB should have this at startx
    g1 = datetime.now()
    load_gwas(db)
    print(f"GWAS loaded - {datetime.now()-g1}")


def magic(file_id: int):
    # create all the stuff

    FILEPATH = f"../uploads/{file_id}.csv"
    DNA_TABLE = f"dna{file_id}"
    OUTPUT_PATH = f"../reports/report_{file_id}.csv"
    OUTPUT_TABLE = f"report{file_id}"

    stime = datetime.now()
    print(f":::Starting the party")

    print(f"building {file_id}")

    b1 = datetime.now()
    try:
        snp: SNPs = build_snp(FILEPATH)  # very slow. very very
    except Exception:
        os.remove(FILEPATH)
        raise Exception("error building snp")
    print(f"snp built - {datetime.now()-b1}")

    db: Engine = create_engine(DB_URL)

    m1 = datetime.now()
    load_myheritage(db, snp, DNA_TABLE)
    print(f"SNP db loaded - {DNA_TABLE} - {datetime.now()-m1}")

    o = datetime.now()
    create_output(db, OUTPUT_TABLE, DNA_TABLE)
    print(f"Tables merged - {datetime.today()-o}")

    x = datetime.now()
    generate_report(db, OUTPUT_TABLE, OUTPUT_PATH)
    print(f"Report generated - {datetime.today()-x}")

    # remove non-standard tables and files
    os.remove(FILEPATH)
    db.execute(f"""DROP TABLE {DNA_TABLE};""")
    db.execute(f"""DROP TABLE {OUTPUT_TABLE};""")

    print(f":::End of party - {file_id} - {datetime.now()-stime}")


def build_snp(fname):
    snp = SNPs(fname)  # sometimes errors out with som pandas C errors?
    # exampleerror: ValueError: invalid literal for int() with base 10: 'GG'
    # pandas.errors.ParserError: Error tokenizing data. C error: Expected 4 fields in line 413885, saw 6

    if not snp.valid or snp.source != "MyHeritage":
        raise Exception("Errors during build. DF not valid or myheritage")

    # so slow. can we speed up remapping
    if snp.assembly != "GRCh38":
        print("Remapping to HRCh38")
        snp.remap(38)
    return snp


def load_myheritage(db, snp, table_name):
    snp.snps[["chrom", "genotype"]] = snp.snps[["chrom", "genotype"]].astype("string")
    snp.snps.to_sql(table_name, db)


def load_gwas(db):
    gwas = pd.read_table(
        GWAS_FILEPATH,
        dtype={
            "REPLICATION SAMPLE SIZE": "string",
            "CHR_POS": "string",
            "SNP_ID_CURRENT": "string",
        },
    )
    gwas.to_sql(GWAS_TABLE, db)  # if_exist=replace?


def create_output(db, table_name, dna_table):
    db.execute(
        f"""
        CREATE TABLE {table_name} AS 
        SELECT {dna_table}.rsid                                  AS rsid,
                {dna_table}.chrom                                AS chromosome,
                {dna_table}.pos                                  AS position,
                {dna_table}.genotype                             AS genotype,
                {GWAS_TABLE}.`REPORTED GENE(S)`                         AS study_mgene,
                {GWAS_TABLE}.MAPPED_GENE                                AS study_gene,
                substr({GWAS_TABLE}.`STRONGEST SNP-RISK ALLELE`,-1)     AS study_allele,
                {GWAS_TABLE}.DATE                                       AS study_date,
                {GWAS_TABLE}.MAPPED_TRAIT                               AS study_mtrait, 
                {GWAS_TABLE}.`DISEASE/TRAIT`                            AS study_trait,
                {GWAS_TABLE}.STUDY                                      AS study_name,
                {GWAS_TABLE}.LINK 
        FROM {dna_table} 
            LEFT JOIN {GWAS_TABLE} 
                ON {dna_table}.rsid = {GWAS_TABLE}.SNPS;"""
    )


def generate_report(db, table_name, filename):
    df = pd.read_sql_table(table_name, db)
    df.to_csv(filename)
