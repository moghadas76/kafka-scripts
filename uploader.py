import datetime
import os, uuid, pathlib, glob, logging, sys, dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

dotenv.load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
file_hd = logging.FileHandler("./uploader.log", mode="a")
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(file_hd)


def is_date(s):
    try:
        datetime.datetime.strptime(s, '%Y/%m/%d')
        return True
    except ValueError:
        return False
    
def is_for_today(s):
    return datetime.datetime.strptime(s, '%Y/%m/%d').date() < datetime.date.today()

class Uploader:

    connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    @classmethod
    def upload_file(cls, file):
        local_file_year, local_file_month, local_file_day = file.split("/")[-3:]
        local_file = f"./{local_file_year}/{local_file_month}/{local_file_day}.txt"

        # Create a file in the local data directory to upload and download

        # Write text to the file

        # Create a blob client using the local file name as the name for the blob
        blob_client = cls.blob_service_client.get_blob_client(container=os.environ["CONTAINER_NAME"], blob=local_file)


        # Upload the created file
        with open(file=file, mode="rb") as data:
            blob_client.upload_blob(data)

                # Create a blob client using the local file name as the name for the blob
        

        logger.info(f"\nUploading {file} to Azure Storage as blob:\n\t" + local_file)



    @staticmethod
    def upload_files(dir_path: str):
        for file in os.listdir(dir_path):
            Uploader.upload_file(os.path.join(dir_path, file))
        

try:
    print("Azure Blob Storage Python quickstart sample")
    path_file = pathlib.Path(os.environ["TARGET_DIR"]) / "cache.txt"
    path_file.touch(exist_ok=True)
    while 1:
        files = glob.glob(os.environ["TARGET_DIR"] + "/**", recursive=True)
        dirs = list((filter(lambda x: is_date("/".join(x.split("/")[-3:])), files)))
        cached_files = None
        with open(path_file, 'r') as f:
            cached_files = set(map(lambda item: item.strip(), f.readlines()))
        logger.info(f"Cached files: {cached_files}, dirs: {dirs}")
        for dir in dirs:
            dir_name = "/".join(dir.split("/")[-3:])
            if dir_name not in cached_files and not is_for_today(dir_name):
                logger.info(f"Uploading new dir: {dir} ...")
                Uploader.upload_files(dir)
                with open(path_file, 'a') as f:
                    f.write(dir_name + "\n")
        logger.info("Watching for new file...")
    # Quickstart code goes here

except Exception as ex:
    logger.error(f'Exception: \n{ex}', exc_info = ex)
