from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

from fastapi import BackgroundTasks, FastAPI
from snps import SNPs
from starlette.responses import Response
import os
from os import environ as env
import psycopg2.pool

from urllib.request import urlretrieve

GWAS_URL = "https://www.ebi.ac.uk/gwas/api/search/downloads/alternative"
GWAS_FILEPATH = f"gwas_catalog.tsv"
GWAS_TABLE = "gwas"

app = FastAPI()
origins = ["null"]  # only known ones
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lets check DB connection on startup for now.
db = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=20,
    dbname=env.get("DB_NAME"),
    user=env.get("DB_USER"),
    password=env.get("DB_PWD"),
    host=env.get("DB_HOST"),
    port=env.get("DB_PORT"),
)


def db_execute(statement: str):
    with db.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute(statement)
    conn.close()


try:
    db_execute("SELECT 1;")
except Exception as err:
    # hide credentials
    raise Exception("Couldnt connect with the DB. Review credentials")


@app.get("/")
async def read_root():
    return Response(
        f"We are running",
        status_code=200,
    )


@app.get("/gwas")
async def gwas_endpoint(bt: BackgroundTasks):
    bt.add_task(urlretrieve, url=GWAS_URL, filename=GWAS_FILEPATH)
    return Response("Triggered", status_code=200)


@app.get("/setup")
async def setup_database(bt: BackgroundTasks):
    bt.add_task(database_setup)
    return Response("Triggered", status_code=200)


@app.post("/candy/{file_id}")
async def process_file(bt: BackgroundTasks, file_id: int):
    bt.add_task(magic, file_id=file_id)
    return Response("Started", status_code=202)


def database_setup() -> None:
    g1 = datetime.now()

    with open("/Users/ps/repos/traits-and-genes/backend/test.sql") as fcreate:
        db_execute(fcreate.read())

    with db.getconn() as conn:
        with conn.cursor() as cur:
            with open(
                "/Users/ps/repos/traits-and-genes/backend/gwas_catalog.tsv"
            ) as fpopulate:
                cur.copy_from(fpopulate, "gwas")
    conn.close()
    print(f"GWAS loaded - {datetime.now()-g1}")


def magic(file_id: int) -> None:

    FILEPATH = f"/Users/ps/repos/traits-and-genes/uploads/{file_id}.csv"
    DNA_TABLE = f"dna{file_id}"
    OUTPUT_PATH = f"/Users/ps/repos/traits-and-genes/reports/report_{file_id}.csv"
    OUTPUT_TABLE = f"report{file_id}"

    stime = datetime.now()
    print(f":::Starting the party : {file_id}")

    b1 = datetime.now()
    try:
        snp: SNPs = build_snp(FILEPATH)  # very slow. very very
    except Exception as err:
        os.remove(FILEPATH)
        raise err

    print(f"snp built - {datetime.now()-b1}")

    m1 = datetime.now()
    load_myheritage(snp, DNA_TABLE)
    del snp
    print(f"SNP database loaded - {DNA_TABLE} - {datetime.now()-m1}")

    o = datetime.now()
    create_output(OUTPUT_TABLE, DNA_TABLE)
    print(f"Tables merged - {datetime.today()-o}")

    x = datetime.now()
    generate_report(OUTPUT_TABLE, OUTPUT_PATH)
    print(f"Report generated - {datetime.today()-x}")

    # remove non-standard tables and files
    os.remove(FILEPATH)
    db_execute(f"DROP TABLE {OUTPUT_TABLE};")
    print(f":::End of party - {file_id} - {datetime.now()-stime}")


def build_snp(fname: str) -> SNPs:
    try:
        with open(fname, "rb") as f:
            snp = SNPs(f.read())  # sometimes errors out with som pandas C errors?
    except Exception as err:
        raise err

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

    db_execute(
        f"create table {table_name} (rsid varchar,chrom varchar,pos varchar,genotype varchar);"
    )

    with db.getconn() as conn:
        with conn.cursor() as cur:
            with open(filename) as f:
                cur.copy_from(file=f, table=table_name, sep=",")

    # leaving contexts doesn't close the connection
    conn.close()
    os.remove(filename)


def create_output(table_name: str, dna_table: str) -> None:

    db_execute(
        f"""
                CREATE TABLE {table_name} AS 
                SELECT DISTINCT {dna_table}.rsid                                  AS rsid,
                        {dna_table}.chrom                                AS chromosome,
                        {dna_table}.pos                                  AS position,
                        {dna_table}.genotype                             AS genotype,
                        {GWAS_TABLE}."REPORTED GENE(S)"                         AS study_mgene,
                        {GWAS_TABLE}."MAPPED_GENE"                                AS study_gene,
                        substr({GWAS_TABLE}."STRONGEST SNP-RISK ALLELE",-1)     AS study_allele,
                        {GWAS_TABLE}."DATE"                                       AS study_date,
                        {GWAS_TABLE}."MAPPED_TRAIT"                               AS study_mtrait, 
                        {GWAS_TABLE}."DISEASE/TRAIT"                            AS study_trait,
                        {GWAS_TABLE}."STUDY"                                      AS study_name,
                        {GWAS_TABLE}."LINK" 
                FROM {dna_table} 
                    LEFT JOIN {GWAS_TABLE} 
                        ON {dna_table}.rsid = {GWAS_TABLE}."SNPS"
                WHERE {GWAS_TABLE}."MAPPED_TRAIT" IS NOT NULL;"""
    )


def generate_report(table_name: str, filename: str) -> None:
    with db.getconn() as conn:
        with conn.cursor() as cur:
            with open(filename, "w") as f:
                cur.copy_to(f, table_name, sep=",")
    conn.close()
