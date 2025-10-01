import os
from pathlib import Path

import dotenv
import pandas as pd
from tqdm import tqdm

from models.Repo import Repo
from processing_pipeline.keyword_matching.services.GithubDataFetcher import GithubDataFetcher

dotenv.load_dotenv()

repos = [
    "/google/deepvariant",
    "/root-project/root",
    "/OpenGene/fastp",
    "/scverse/scanpy",
    "/allenai/scispacy",
    "/broadinstitute/gatk",
    "/qutip/qutip",
    "/soedinglab/MMseqs2",
    "/su2code/SU2",
    "/tum-pbs/PhiFlow",
    "/scipipe/scipipe",
    "/openbabel/openbabel",
    "/qupath/qupath",
    "/broadinstitute/cromwell",
    "/hail-is/hail",
    "/psi4/psi4",
    "/CliMA/Oceananigans.jl",
    "/sofa-framework/sofa",
    "/stardist/stardist",
    "/COMBINE-lab/salmon",
]


def query_and_save_versions(package_versions_path):
    os.makedirs(package_versions_path.parent, exist_ok=True)
    data = []
    for repo_path in tqdm(repos, "Getting latest repository version"):
        _, author, repo = repo_path.split("/")
        git_repo = GithubDataFetcher(os.getenv("GITHUB_TOKEN"), Repo(
            author=author,
            name=repo,
            version="latest"
        ))
        info = git_repo.get_repo_info()
        data.append(dict(author=author, repo=repo, version=info.latest_version, wiki=info.homepage))

    df = pd.DataFrame(data)
    df.to_csv(package_versions_path, index=False)


def print_as_repo_objects(package_versions_path: Path, save_to: Path):
    df = pd.read_csv(package_versions_path)
    with open(save_to, "w", encoding="utf-8") as f:
        f.write("credential_list: List[Repo] = [\n")
        for (author, repo, version, wiki) in df.itertuples(index=False):
            f.write(f"Repo.from_dict({Repo(author=author, name=repo, version=version, wiki=wiki if isinstance(wiki, str) else None)}),\n")
        f.write("]")


def main():
    package_versions_path = Path("data/repo_info/repo_info.csv")
    # query_and_save_versions(package_versions_path)

    print_as_repo_objects(package_versions_path, package_versions_path.with_suffix(".py"))




if __name__ == "__main__":
    main()
