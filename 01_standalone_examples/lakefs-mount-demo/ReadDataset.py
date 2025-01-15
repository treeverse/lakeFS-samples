import argparse
# Initialize parser
parser = argparse.ArgumentParser()
parser.add_argument("--mount_location", help="Location for lakeFS Mount")
parser.add_argument("--dataset_name", help="Hugging Face dataset name")
args = parser.parse_args()

mount_location = args.mount_location
hugging_face_dataset_name = args.dataset_name

# Load the dataset from lakeFS Mount location
from datasets import load_from_disk, load_dataset
dataset = load_from_disk(f'{mount_location}/{hugging_face_dataset_name}')
print(dataset)
