"""
# Given a myheritage.csv results file:
- return known traits/studies for those rsids
    - GET list of rsid from frontend, query database on backend. 
- return set of GENES for the given rsids


- Change gmail. size not big enough. make sure gmail is not going to close my account. use other account
- from backgorund task to redis+rq (so we can do more than 1)
- decide what to include in final output
- improve error messages. Instead of error, specify where/why theres an error
- react/vue frontend
- add about page - purpose, donation, data protection


- DB stuff
    - Full analysis of queries. Create index where apropiate
        - Traits query SELECT * FROM WHERE rsid in (VERY LARGE). Temp table vs large in.
        - Genes query
    -
    - don run genes and gwas every fucking time. load from backup
    - speed up genes addition. index on positions or txstart/end?


"""
import mimetypes
import os
import re
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from random import choice
from string import ascii_lowercase
from tempfile import NamedTemporaryFile
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
from fastapi import BackgroundTasks, Body, FastAPI, File, Form, UploadFile, status
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from snps import SNPs
from sqlalchemy import create_engine

FILESIZE = 40_000_000  # ~mb
EMAIL_PATTERN = re.compile(
    r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
KEEP_FILES = True
SEND_EMAIL = False

app = FastAPI()
app.mount("/htmls", StaticFiles(directory="htmls"), name="htmls")
origins = ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root():
    return FileResponse("htmls/index.html")


@app.get("/good")
async def success():
    return FileResponse("htmls/good.html")


@app.get("/bad")
async def failure():
    return FileResponse("htmls/bad.html")


async def validate_request(email, file):

    if not EMAIL_PATTERN.match(email):
        print("bad email")
        return None

    if file.filename[-4:] != ".csv":
        print("bad format")
        return None

    fline = file.file.readline()
    file.file.seek(0)
    if b"##fileformat=MyHeritage" not in fline:
        print("bad first line", fline)
        return None

    filename = generate_filename()
    real_file_size = 0
    with NamedTemporaryFile(delete=False) as temp:
        for chunk in file.file:
            real_file_size += len(chunk)
            if real_file_size > FILESIZE:
                print("too big")
                return None
            temp.write(chunk)
        shutil.move(temp.name, filename)
    return filename


@app.post("/qq")
async def query_rsids(rsids=Body(...)):
    # query db for these RSIDS. return file?
    return {"got": rsids}


@app.post("/candy")
async def process_file(
    bt: BackgroundTasks, candy_address: str = Form(...), candy: UploadFile = File(...)
):

    filename = await validate_request(candy_address, candy)

    if not filename:
        return RedirectResponse(url="/bad", status_code=status.HTTP_303_SEE_OTHER)

    # Trigger background task to run the party and send email
    print("hey yeeey")
    # bt.add_task(magic, filename=filename, email=candy_address)

    return RedirectResponse(url="/good", status_code=status.HTTP_303_SEE_OTHER)


def magic(filename, email):
    # create all the stuff
    # send email with good or bad

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

    if SEND_EMAIL and ("gmail" in email or "hotmail" in email):
        send_email(email, filename)

    if not KEEP_FILES:
        os.remove(filename)
        os.remove(DB_FILE)
        print("files removed")

    print(email, filename)
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
    snp.snps[["chrom", "genotype"]] = snp.snps[[
        "chrom", "genotype"]].astype("string")
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


def send_email(to, attachment, emailfrom="noreply@genalytics.com"):
    print(":::Start email")
    FROM = emailfrom
    TO = to
    fileToSend = attachment

    msg = MIMEMultipart()
    msg["Subject"] = "Your Genalytics results"
    msg["From"] = FROM
    msg["To"] = TO
    msg.preamble = "The results are attached"

    ctype, encoding = mimetypes.guess_type(fileToSend)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    _, subtype = ctype.split("/", 1)

    with open(fileToSend) as fp:
        attachment = MIMEText(fp.read(), _subtype=subtype)

    attachment.add_header("Content-Disposition",
                          "attachment", filename=fileToSend)
    msg.attach(attachment)

    print("Sending...")
    with smtplib.SMTP("smtp.gmail.com:587") as server:
        server.starttls()
        server.login("pablosr11@gmail.com", "pjbgwxkakhapluhi")
        server.sendmail(FROM, TO, msg.as_string())

    print(":::Done sending email")


def generate_filename():
    return "tmp/" + "".join([choice(ascii_lowercase) for _ in range(10)]) + ".csv"
