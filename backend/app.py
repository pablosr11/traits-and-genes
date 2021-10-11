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
GWAS_FILEPATH = f"gwas_catalog.tsv"
GWAS_TABLE = "gwas"
DB_URL = "postgresql://ps@localhost:5432/dna"

app = FastAPI()
origins = ["null"]
db: Engine = create_engine(DB_URL, poolclass=QueuePool)
pool: QueuePool = db.pool

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root():
    print(pool.status())
    return Response(
        f"We are running - {pool.size()} - {pool.overflow()} - {pool.checkedout()}",
        status_code=200,
    )


@app.get("/gwas")
async def gwas_endpoint(bt: BackgroundTasks):
    # issue with new line at EOF
    # issue with lines getting split up 187218
    bt.add_task(urlretrieve, url=GWAS_URL, filename=GWAS_FILEPATH)
    return Response("Triggered", status_code=200)


@app.get("/setup")
async def setup_database(bt: BackgroundTasks):
    bt.add_task(database_setup)
    return Response("Triggered", status_code=200)


@app.post("/candy/{file_id}")
async def process_file(bt: BackgroundTasks, file_id: int):

    # Trigger background task to run the party
    bt.add_task(magic, file_id=file_id)

    return Response("Started", status_code=202)


def database_setup() -> None:
    g1 = datetime.now()

    conn = pool.connect()
    cur = conn.cursor()
    with open("/Users/ps/repos/traits-and-genes/backend/test.sql") as fcreate:
        cur.execute(fcreate.read())
    with open("/Users/ps/repos/traits-and-genes/backend/gwas_catalog.tsv") as fpopulate:
        cur.copy_from(fpopulate, "gwas")
    conn.commit()
    conn.close()
    print(f"GWAS loaded - {datetime.now()-g1}")


def magic(file_id: int) -> None:

    FILEPATH = f"/Users/ps/repos/traits-and-genes/uploads/{file_id}.csv"
    DNA_TABLE = f"dna{file_id}"
    OUTPUT_PATH = f"/Users/ps/repos/traits-and-genes/report_{file_id}.csv"
    OUTPUT_TABLE = f"report{file_id}"

    stime = datetime.now()
    print(f":::Starting the party")

    print(f"building {file_id}")

    b1 = datetime.now()
    try:
        snp: SNPs = build_snp(FILEPATH)  # very slow. very very
    except Exception as err:
        os.remove(FILEPATH)
        raise err
    print(f"snp built - {datetime.now()-b1}")

    m1 = datetime.now()
    load_myheritage(snp, DNA_TABLE)
    print(f"SNP database loaded - {DNA_TABLE} - {datetime.now()-m1}")

    o = datetime.now()
    create_output(OUTPUT_TABLE, DNA_TABLE)
    print(f"Tables merged - {datetime.today()-o}")

    x = datetime.now()
    generate_report(OUTPUT_TABLE, OUTPUT_PATH)
    print(f"Report generated - {datetime.today()-x}")

    # remove non-standard tables and files
    os.remove(FILEPATH)
    db.execute(f"""DROP TABLE {DNA_TABLE};""")
    db.execute(f"""DROP TABLE {OUTPUT_TABLE};""")

    print(f":::End of party - {file_id} - {datetime.now()-stime}")


def build_snp(fname: str) -> SNPs:
    try:
    snp = SNPs(fname)  # sometimes errors out with som pandas C errors?
    except Exception as err:
        raise err
    # exampleerror: ValueError: invalid literal for int() with base 10: 'GG'
    # pandas.errors.ParserError: Error tokenizing data. C error: Expected 4 fields in line 413885, saw 6

    if not snp.valid or snp.source != "MyHeritage":
        raise Exception("Errors during build. DF not valid or myheritage")

    # so slow. can we speed up remapping
    if snp.assembly != "GRCh38":
        print("Remapping to HRCh38")
        snp.remap(38)
    return snp


def load_myheritage(snp: SNPs, table_name: str) -> None:
    snp.snps[["chrom", "genotype"]] = snp.snps[["chrom", "genotype"]].astype("string")
    filename = f"/Users/ps/repos/traits-and-genes/backend/{table_name}.csv"

    # issue: pandas default to sqlite when using DBAPI 2.0. we will write to CSV and load that to psql
    snp.snps.to_csv(filename)

    conn = pool.connect()
    cur = conn.cursor()
    cur.execute(
        f"""
        create table {table_name} (
            rsid varchar,
            chrom varchar,
            position varchar,
            result varchar
    );"""
    )
    with open(filename) as f:
        cur.copy_from(file=f, table=table_name, sep=",")
    conn.commit()
    conn.close()

    os.remove(filename)


def create_output(table_name: str, dna_table: str) -> None:
        f"""
        CREATE TABLE {table_name} AS 
        SELECT DISTINCT {dna_table}.rsid                                  AS rsid,
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
                ON {dna_table}.rsid = {GWAS_TABLE}.SNPS
        WHERE {GWAS_TABLE}.MAPPED_TRAIT IS NOT NULL;"""
    )


def generate_report(table_name: str, filename: str) -> None:
    df.to_csv(filename)
