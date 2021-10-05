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
    return Response(status_code=200)


@app.get("/gwas")
async def gwas_endpoint(bt: BackgroundTasks):
    bt.add_task(urlretrieve, url=GWAS_URL, filename=GWAS_FILEPATH)
    return Response("Triggered", status_code=200)


    # Trigger background task to run the party
    print("hey yeeey")
    # bt.add_task(magic, filename=filename)

    return Response("Started", status_code=202)


def magic(filename):
    # create all the stuff

    print(f":::Starting the party - {datetime.now()}")

    table_name = filename.split("/")[1][:-4]

    print(f"building {table_name}")

    snp = build_snp(filename)

    DB_FILE = f"tmpdb/{table_name}.db"
    db = create_engine(f"sqlite:///{DB_FILE}")

    MYHERITAGE_TABLE = load_myheritage(db, snp)

    # These two should be independent. DB should have this at startx
    GWAS_TABLE = load_gwas(db)
    # GENES_TABLE = load_genes(db)

    # join_genes(db, MYHERITAGE_TABLE, GENES_TABLE)

    create_output(db, table_name, MYHERITAGE_TABLE, GWAS_TABLE)

    generate_report(db, table_name, filename)

    print(filename)
    print(f":::End of party - {datetime.now()}")


def build_snp(fname):
    snp = SNPs(fname)

    if not snp.valid or snp.source != "MyHeritage":
        raise Exception("Errors during build. DF not valid or myheritage")

    if snp.assembly != "GRCh38":
        print("Remapping to HRCh38")
        snp.remap(38)
    return snp


def load_myheritage(db, snp):
    MYHERITAGE_TABLE = "myheritage"
    snp.snps[["chrom", "genotype"]] = snp.snps[["chrom", "genotype"]].astype("string")
    snp.snps.to_sql(MYHERITAGE_TABLE, db)
    print(f"{MYHERITAGE_TABLE} imported!")
    return MYHERITAGE_TABLE


def load_gwas(db):
    GWAS_FILE = "gwas_catalog_v1.0.2-associations_e100_r2021-05-19.tsv"
    GWAS_TABLE = "gwas"
    gwas = pd.read_table(
        GWAS_FILE,
        dtype={
            "REPLICATION SAMPLE SIZE": "string",
            "CHR_POS": "string",
            "SNP_ID_CURRENT": "string",
        },
    )
    gwas.to_sql(GWAS_TABLE, db)
    print(f"{GWAS_TABLE} imported!")
    return GWAS_TABLE


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


def create_output(db, table_name, MYHERITAGE_TABLE, GWAS_TABLE):
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
                {GWAS_TABLE}.link 
        FROM {MYHERITAGE_TABLE} 
            LEFT JOIN {GWAS_TABLE} 
                ON {MYHERITAGE_TABLE}.rsid = {GWAS_TABLE}.SNPS;"""
    )
    # Can be added if genes are joined earlier
    # {MYHERITAGE_TABLE}.gene                                 AS gene,
    # {MYHERITAGE_TABLE}.geneName                             AS geneName,

    print(f"{table_name} created")


def generate_report(db, table_name, filename):
    df = pd.read_sql_table(table_name, db)
    df.to_csv(filename)
