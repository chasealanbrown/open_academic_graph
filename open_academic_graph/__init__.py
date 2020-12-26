import os
import pandas as pd
import numpy as np
from zipfile import ZipFile
#import gzip
import json
#import csv
import sqlite3
import requests
from tqdm import tqdm
import logging
#from odo import odo, discover, resource

LOG = logging.getLogger("Open Academic Graph Downloader")
logging.basicConfig(level=logging.DEBUG,
                    #  - %(name)s - %(levelname)s -
                    format="%(asctime)s - %(message)s")


def sizeof_fmt(num, suffix='B'):
  for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
    if abs(num) < 1024.0:
      return "%3.1f%s%s" % (num, unit, suffix)
    num /= 1024.0
  return "%.1f%s%s" % (num, 'Yi', suffix)


def download_url(url, filepath, params = None):
  """
  Summary:
    Download a file from a specific url with a progress bar.
  Source:
    https://stackoverflow.com/questions/56795227/
    how-do-i-make-progress-bar-while-downloading-file-in-python#56796119
  """
  LOG.debug(f"Downloading {filepath} from {url} ...")
  filename = os.path.basename(filepath)
  with requests.get(url, stream=True, params = params) as r:
    r.raise_for_status()

    # Get online file size
    datasize = r.headers.get("Content-Length")
    if datasize is None:
      datasize = r.headers.get("content-length")

    # Check if file already exists - if so, don't download agagin
    if os.path.exists(filepath):
      # Check online file size vs. local file size
      filesize = os.path.getsize(filepath)
      LOG.debug(f"File size of local file: {sizeof_fmt(filesize)}")
      if int(datasize) == filesize:
        LOG.info("File already on local disk")
        return filepath

    # Download file with progress bar in chunks
    LOG.info(f"File size of Download: {sizeof_fmt(int(datasize))}")
    with open(filepath, 'wb') as f:
      pbar = tqdm(total = None if datasize is None else int(datasize),
                  desc = f"Downloading {filename}")
      for chunk in r.iter_content(chunk_size = 8192):
        if chunk:  # filter out keep-alive new chunks
          f.write(chunk)
          pbar.update(len(chunk))
  return filepath


class Database(object):

  def __init__(self,
               download_dir = "/data/databases/open_academic_graph/downloaded",
               db_path = "/data/databases/open_academic_graph/open_academic_graph.db"): #"data/open_academic_graph.db"):
    self.db_path = db_path
    self.download_dir = download_dir
#    self.download_dir = os.path.join("data", "downloaded")

    self._cx = sqlite3.connect(str(self.db_path))
    self._cursor = self._cx.cursor()

    if not self._is_initialized():
      self._build_database()


  def _build_database(self):

    # Initialize directories to put data into
    os.makedirs("data",
                exist_ok = True)
    os.makedirs(self.download_dir,
                exist_ok = True)

    # First enure that the databse is removed and reinitialized,
    #   such that we start from scratch
    LOG.info("Starting database creation...")
#    os.remove(self.db_path)
#    self._cx = sqlite3.connect(str(self.db_path))
#    self._cursor = self._cx.cursor()


    # online location and schemas of files
    base_url = "https://academicgraphv2.blob.core.windows.net/oag/"
    file_links = [("linkage/venue_linking_pairs.zip", "venue_pairs"),
                  ("linkage/paper_linking_pairs.zip", "pairs"),
                  ("linkage/author_linking_pairs.zip", "pairs"),
                  ("aminer/venue/aminer_venues.zip", "venue"),
                  ("mag/venue/mag_venues.zip", "venue"),
                  ("mag/paper/mag_papers_0.zip", "paper"),
                  ("mag/paper/mag_papers_1.zip", "paper"),
                  ("mag/paper/mag_papers_2.zip", "paper"),
                  ("aminer/paper/aminer_papers_0.zip", "paper"),
                  ("aminer/paper/aminer_papers_1.zip", "paper"),
                  ("aminer/paper/aminer_papers_2.zip", "paper"),
                  ("aminer/paper/aminer_papers_3.zip", "paper"),
                  ("mag/author/mag_authors_0.zip", "author"),
                  ("mag/author/mag_authors_1.zip", "author"),
                  ("mag/author/mag_authors_2.zip", "author"),
                  ("aminer/author/aminer_authors_0.zip", "author"),
                  ("aminer/author/aminer_authors_1.zip", "author"),
                  ("aminer/author/aminer_authors_2.zip", "author"),
                  ("aminer/author/aminer_authors_3.zip", "author")]

    # Column names, datatype, and description of each table type
    # ("Field Name","Field Type","Description")
    schemas = {
        # Added "id" column for unique id
        "venue_pairs": [("id", "int", "linking index"),
                        ("mid", "string", "mag id"),
                        ("aid", "string", "aminer id")],
        # Added "id" column for unique id
        "pairs": [("id", "int", "linking index"),
                  ("mid", "int", "mag id"),
                  ("aid", "string", "aminer id")],
        "venue": [("id", "string", "venue id"),
                  ("JournalId", "string", "journal id"),
                  ("ConferenceId", "string", "conference id"),
                  ("DisplayName", "string", "venue name"),
                  ("NormalizedName", "string", "normalized venue name")],
        "paper": [("id","string","paper ID"),
                  ("title","string","paper title"),
                  # Foriegn key - replaced '.' with '_'
                  ("authors_name","string","author name"),
                  # Foriegn key - replaced '.' with '_'
                  ("author_org","string","author affiliation"),
                  # Foriegn key - replaced '.' with '_'
                  ("author_id","string","author ID"),
                  # Foriegn key - replaced '.' with '_'
                  ("venue_id","string","paper venue ID"),
                  # Foriegn key - replaced '.' with '_'
                  ("venue_raw","string","paper venue name"),
                  ("year","int","published year"),
                  ("keywords","list of strings","keywords"),
                  ("n_citation","int","citation number"),
                  ("page_start","string","page start"),
                  ("page_end","string","page end"),
                  ("doc_type","string","paper type: journal, book title…"),
                  ("lang","string","detected language"),
                  ("publisher","string","publisher"),
                  ("volume","string","volume"),
                  ("issue","string","issue"),
                  ("issn","string","issn"),
                  ("isbn","string","isbn"),
                  ("doi","string","doi"),
                  ("pdf","string","pdf URL"),
                  ("url","list","external links"),
                  ("abstract","string","abstract")],
        "author":[("id","string","author id"),
                  ("name","string","author name"),
                  ("normalized_name","string","normalized author name"),
                  ("orgs","list of strings","author affiliations"),
                  ("org","string","last known affiliation"),
                  ("position","string","author position"),
                  ("n_pubs","int","the number of author publications"),
                  ("n_citation","int","author citation count"),
                  ("h_index","int","author h-index"),
                  # Foriegn key - replaced '.' with '_'
                  ("tags_t","string","research interests"),
                  # Foriegn key - replaced '.' with '_'
                  ("tags_w","int","weight of interests"),
                  # Foriegn key - replaced '.' with '_'
                  ("pubs_i","string","author paper id"),
                  # Foriegn key - replaced '.' with '_'
                  ("pubs_r","int","author order in the paper")]}

    # Download and add all the files to the database
    pbar = tqdm(file_links)
    for file_link, schema_type in pbar:
      pbar.set_description(f"Processing {file_link}")

      # Download
      url = base_url + file_link
      filename = file_link.split("/")[-1]
      path = os.path.join(self.download_dir, filename)
      download_url(url = url, filepath = path)

      # Create table
      tbl_name = os.path.splitext(os.path.basename(filename))[0]
      cols_str = ','.join([f"{col} {dtype} PRIMARY KEY"
                           if col_ix == 0
                           else f"{col} {dtype}"
                           for col_ix, (col, dtype, _)
                           in enumerate(schemas[schema_type])])

      LOG.debug(f"Creating table {tbl_name} if it doesn't exist...")
      create_tbl_cmd = f"create table if not exists {tbl_name}({cols_str})"
      self._cursor.execute(create_tbl_cmd)

      # Insert into table
      with ZipFile(path) as z:

        # First check if table already has all data in it
        tbl_populated = False
        with z.open(z.namelist()[0]) as f:
          LOG.info(f"Checking file and table {tbl_name} size...")
          if tbl_name == "paper_linking_pairs":
            nlines = 91137597
          elif tbl_name == "mag_papers_0":
            nlines = 21406986
          else:
            LOG.debug("Reading file to get number of lines...")
            nlines = sum(1 for line in f)
          if "papers" not in tbl_name:
            row_count = self.last_id_count(tbl_name)
          else:
            row_count = self.count(tbl_name)

          LOG.debug(f"number of lines in file: {nlines}\
                    \n- number of rows in table: {row_count}")
          if row_count == nlines:
            LOG.info(f"Table {tbl_name} already populated.")
            tbl_populated = True

        if not tbl_populated:
          with z.open(z.namelist()[0]) as f:

            # Generator for reading lines
            def json_generator(line_iter):
              for line_ix, line in enumerate(line_iter):
                line_dict = dict(json.loads(line))
                # Ensure that the ordering is correct
                #   and that missing values are dealt with
                if "id" not in line_dict:
                  line_dict["id"] = line_ix
                vals = [line_dict.get(col) for col, _, __ in schemas[schema_type]]
                yield tuple(vals)


            num_cols = len(schemas[schema_type])
            val_insrt = ','.join(num_cols * ['?'])
            sql_cmd = f"insert into {tbl_name} values ({val_insrt})"

            self._cursor.executemany(sql_cmd,
                                     tqdm(json_generator(f),
                                          total=nlines,
                                          desc=f"Inserting to {tbl_name}"))
            self._cx.commit()
    return True


  def _is_initialized(self):
    row_count = 0
    try:
      some_table = "aminer_authors_3"#self.tables().iloc[0][0]
      # Only count top 10 entries (no all entries with COUNT(*)), to save time
      count_sql = f"SELECT * FROM {some_table} LIMIT 10"
      row_count = self.query(count_sql).shape[0]
    except Exception as e:
      LOG.info(e)
      LOG.info("Database doesn't appear to be initialized")
      return False
    return row_count>0


  def query(self, sql_str: str = None, **kwargs) -> pd.DataFrame:
    return pd.read_sql(sql=sql_str, con=self._cx, **kwargs)


  def tables(self):
    sql_query = "SELECT name FROM sqlite_master WHERE type='table'"
    return self.query(sql_str=sql_query)


  def last_id_count(self, table_name: str):
    """
    Approximates count(), by assuming that an enumerated 'id' column
        has no gaps
    """
    sql_query = f"SELECT MAX(id) FROM {table_name}"
    last = self.query(sql_str=sql_query).iloc[0][0]
    if type(last) in [int, float, np.int64]:
      # Add one since ids start at 0
      return last + 1
    else:
      LOG.debug("MAX(id) is not an integer - using count instead")
      return self.count(table_name)


  def count(self, table_name: str):
    sql_query = f"SELECT COUNT(id) FROM {table_name}"
    try:
      val = self.query(sql_str=sql_query).iloc[0][0]
      return val
    except:
      sql_query = f"SELECT COUNT(1) FROM {table_name}"
      return self.query(sql_str=sql_query).iloc[0][0]


  def sample(table: str,
             n_rows: int):
    sql_query = f"""
    Declare @n int
    Select @n=count(1) from {table}

    create table RandomKeys (RandomKey int)
    create table RandomKeysAttempt (RandomKey int)

    -- generate m random keys between 1 and n
    for i = 1 to {n_rows}
      insert RandomKeysAttempt select rand()*n + 1

      -- eliminate duplicates
      insert RandomKeys select distinct RandomKey from RandomKeysAttempt

      -- as long as we don't have enough, keep generating new keys,
      -- with luck (and m much less than n), this won't be necessary
      while count(RandomKeys) < {n_rows}
        NextAttempt = rand()*n + 1
          if not exists (select * from RandomKeys where RandomKey = NextAttempt)
              insert RandomKeys select NextAttempt

              -- get our random rows
              select *
              from RandomKeys r
              join {table} t ON r.RandomKey = t.UniqueKey
    """
    return self.query(sql=sql_query)


  def __del__(self):
    self._cx.close()
