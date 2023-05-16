import pandas as pd


def save_parquet():
    df = pd.read_csv("./data/inf_diario_fi_202304.csv",
                     sep=";", encoding="latin-1")

    df.to_parquet("./data/cotas_parquet.snappy.parquet", index=False)


def read_parquet(path):
    return pd.read_parquet(path)


def main():
    save_parquet()
    df = read_parquet("./data/cotas_parquet.snappy.parquet")

    print(df.head())


if __name__ == "__main__":
    main()
