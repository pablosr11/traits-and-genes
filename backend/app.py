"""
- dont run genes and gwas every time. load from existing DB. 
    - (dont create 309509 db, use one with GWAS, GENES preloaded. Create urSNP one,  )
- include genes
- from backgorund task to redis+rq (so we can do more than 1)

# Given a myheritage.csv results file:
- return known traits/studies for those rsids
    - GET list of rsid from frontend, query database on backend. 
- return set of GENES for the given rsids

- DB stuff
    - Full analysis of queries. Create index where apropiate
        - Traits query SELECT * FROM WHERE rsid in (VERY LARGE). Temp table vs large in.
        - Genes query
    -

    - speed up genes addition. index on positions or txstart/end?


DOWNLOAD GWAS: https://www.ebi.ac.uk/gwas/docs/file-downloads

"""
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
from fastapi import BackgroundTasks, FastAPI
from snps import SNPs
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from starlette.responses import Response

from urllib.request import urlretrieve

GWAS_URL = "https://www.ebi.ac.uk/gwas/api/search/downloads/alternative"
GWAS_FILEPATH = f"resources/gwas_{datetime.today().strftime('%d%m%Y')}.tsv"
MYHERITAGE_TABLE = "myheritage"
GWAS_TABLE = "gwas"

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


@app.post("/candy/{file_id}")
async def process_file(bt: BackgroundTasks, file_id: int):

    # Trigger background task to run the party
    print("hey yeeey")
    bt.add_task(magic, file_id=file_id)

    return Response("Started", status_code=202)


def magic(file_id: int):
    # create all the stuff

    FILEPATH = f"../{file_id}.csv"
    OUTPUT_PATH = f"../report_{file_id}.csv"
    OUTPUT_TABLE = f"report{file_id}"
    DB_FILE = f"db/{file_id}.db"

    stime = datetime.now()
    print(f":::Starting the party")

    print(f"building {file_id}")

    b1 = datetime.now()
    snp: SNPs = build_snp(FILEPATH)  # very slow. very very
    print(f"snp built - {datetime.now()-b1}")

    db: Engine = create_engine(f"sqlite:///{DB_FILE}")

    m1 = datetime.now()
    load_myheritage(db, snp)
    print(f"SNP db loaded - {MYHERITAGE_TABLE} - {datetime.now()-m1}")

    # These two should be independent. DB should have this at startx
    g1 = datetime.now()
    load_gwas(db)
    print(f"GWAS loaded -{datetime.now()-g1}")

    # g2 = datetime.now()
    # GENES_TABLE = load_genes(db)
    # print(f"GENES loaded -{datetime.now()-g2}")
    # join_genes(db, MYHERITAGE_TABLE, GENES_TABLE)

    o = datetime.now()
    create_output(db, OUTPUT_TABLE)
    print(f"Tables merged - {datetime.today()-o}")

    x = datetime.now()
    generate_report(db, OUTPUT_TABLE, OUTPUT_PATH)
    print(f"Report generated - {datetime.today()-x}")

    print(f":::End of party - {file_id} - {datetime.now()-stime}")


def build_snp(fname):
    snp = SNPs(fname)  # sometimes errors out with som pandas C errors?

    if not snp.valid or snp.source != "MyHeritage":
        raise Exception("Errors during build. DF not valid or myheritage")

    # so slow. can we speed up remapping
    if snp.assembly != "GRCh38":
        print("Remapping to HRCh38")
        snp.remap(38)
    return snp


def load_myheritage(db, snp):
    snp.snps[["chrom", "genotype"]] = snp.snps[["chrom", "genotype"]].astype("string")
    snp.snps.to_sql(MYHERITAGE_TABLE, db)


def load_gwas(db):
    gwas = pd.read_table(
        GWAS_FILEPATH,
        dtype={
            "REPLICATION SAMPLE SIZE": "string",
            "CHR_POS": "string",
            "SNP_ID_CURRENT": "string",
        },
    )
    gwas.to_sql(GWAS_TABLE, db)


def load_genes(db):
    GENES_FILE = "geneshg38.tsv"
    GENES_TABLE = "genes"
    genes = pd.read_table(GENES_FILE)
    # Get rid of additional info. chrY_randomfix -> Y
    genes.chrom = genes.chrom.str.replace(
        r"(.*)(chr[a-zA-Z0-9]{1,2})(.*)", lambda x: x.group(2)[3:], regex=True
    )
    genes[["#geneName", "name", "chrom", "strand"]] = genes[
        ["#geneName", "name", "chrom", "strand"]
    ].astype("string")
    genes[["txStart", "txEnd", "cdsStart", "cdsEnd", "exonCount"]] = genes[
        ["txStart", "txEnd", "cdsStart", "cdsEnd", "exonCount"]
    ].astype("int32")
    genes.to_sql(GENES_TABLE, db)
    print(f"{GENES_TABLE} imported!")
    return GENES_TABLE


def join_genes(db, MYHERITAGE_TABLE, GENES_TABLE):
    db.execute(f"""ALTER TABLE {MYHERITAGE_TABLE} ADD COLUMN gene text;""")
    db.execute(f"""ALTER TABLE {MYHERITAGE_TABLE} ADD COLUMN geneName text;""")
    db.execute(
        f"""
            UPDATE {MYHERITAGE_TABLE}
            SET gene = gen, geneName = genNa
            FROM (
                SELECT rsid,`#geneName` AS gen, name AS genNa
                FROM {MYHERITAGE_TABLE}
                    LEFT JOIN {GENES_TABLE} ON
                        {GENES_TABLE}.chrom = {MYHERITAGE_TABLE}.chrom
                        AND {MYHERITAGE_TABLE}.pos BETWEEN {GENES_TABLE}.txStart AND {GENES_TABLE}.txEnd
                GROUP BY rsid) AS qquery
            WHERE {MYHERITAGE_TABLE}.rsid = qquery.rsid;
        """
    )
    print("genes added to myheritage table")


def create_output(db, table_name):
    db.execute(
        f"""
        CREATE TABLE {table_name} AS 
        SELECT {MYHERITAGE_TABLE}.rsid                                  AS rsid,
                {MYHERITAGE_TABLE}.chrom                                AS chromosome,
                {MYHERITAGE_TABLE}.pos                                  AS position,
                {MYHERITAGE_TABLE}.genotype                             AS genotype,
                {GWAS_TABLE}.`REPORTED GENE(S)`                         AS study_mgene,
                {GWAS_TABLE}.MAPPED_GENE                                AS study_gene,
                substr({GWAS_TABLE}.`STRONGEST SNP-RISK ALLELE`,-1)     AS study_allele,
                {GWAS_TABLE}.DATE                                       AS study_date,
                {GWAS_TABLE}.MAPPED_TRAIT                               AS study_mtrait, 
                {GWAS_TABLE}.`DISEASE/TRAIT`                            AS study_trait,
                {GWAS_TABLE}.STUDY                                      AS study_name,
                {GWAS_TABLE}.LINK 
        FROM {MYHERITAGE_TABLE} 
            LEFT JOIN {GWAS_TABLE} 
                ON {MYHERITAGE_TABLE}.rsid = {GWAS_TABLE}.SNPS;"""
    )
    # Can be added if genes are joined earlier
    # {MYHERITAGE_TABLE}.gene                                 AS gene,
    # {MYHERITAGE_TABLE}.geneName                             AS geneName,


def generate_report(db, table_name, filename):
    df = pd.read_sql_table(table_name, db)
    df.to_csv(filename)
