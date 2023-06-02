from dagster import Config

class LakeFSOpConfig(Config):
    repo: str
    sourceBranch: str
    newBranch: str

