import pandas as pd


def main():
    files = [
        "metadata/ast/no_types_allenai_scispacy_v0.5.4.csv",
        "metadata/ast/no_types_qutip_qutip_v5.0.4.csv",
        "metadata/ast/no_types_scverse_scanpy_1.10.1.csv",
    ]
    combined_csv = pd.concat([pd.read_csv(f) for f in files])
    combined_csv.to_csv("metadata/ast/no_types_combined_csv.csv", index=False)


if __name__ == "__main__":
    main()
