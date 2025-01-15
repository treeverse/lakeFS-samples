import argparse
# Initialize parser
parser = argparse.ArgumentParser()
parser.add_argument("--repo_name", help="lakeFS repository name")
parser.add_argument("--branch_name", help="lakeFS branch name")
parser.add_argument("--mount_location", help="Location for lakeFS Mount")
parser.add_argument("--dataset_name", help="Hugging Face dataset name")
parser.add_argument("--number_of_images", help="Number of Images")
args = parser.parse_args()

repo_name = args.repo_name
experimentBranch = args.branch_name
mount_location = args.mount_location
hugging_face_dataset_name = args.dataset_name
number_of_images = int(args.number_of_images)

import lakefs
repo = lakefs.Repository(repo_name)
branchExperiment = repo.branch(experimentBranch)

# Load the dataset from lakeFS Mount location
from datasets import load_from_disk, DatasetDict
dataset = load_from_disk(f'{mount_location}/{hugging_face_dataset_name}')

# Select number of images
dataset=dataset.select(range(number_of_images))
print(dataset)

# Save dataset to lakeFS repo
dataset.save_to_disk(f'lakefs://{repo_name}/{experimentBranch}/datasets/{hugging_face_dataset_name}_subset/')
branchExperiment.commit(message='Uploaded transformed images!', metadata={'using': 'python_sdk'})