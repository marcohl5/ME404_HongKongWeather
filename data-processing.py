import pandas as pd
import os


def process_qc():
    dir_path = "original-data/2023/"
    file_names = ["temp",
                 "maxt",
                 "mint",
                 "rh",
                 "ws",
                 "wd",
                 "60rf",
                 "sol"]

    # Initialize an empty DataFrame
    df_merged = pd.DataFrame()

    for file in file_names:
        file_path = os.path.join(dir_path, f"{file}.csv")

        # Read the CSV file
        df = pd.read_csv(file_path)

        # Ensure "obstime" and "station_id" columns exist
        if not {"obstime", "station_id"}.issubset(df.columns):
            raise ValueError(f"Missing required columns in {file}.csv")

        # Filter for station_id == 6087 (Chosen location)
        df = df[df["station_id"] == 6087]
        df = df.drop(["station_id"], axis=1)

        # Save as separate DF
        df.to_csv(f"processed-data/{file}_filt.csv")
        print(f"{file} saved")

        # Merge on "obstime"
        if df_merged.empty:
            df_merged = df
        else:
            df_merged = pd.merge(df_merged,
                                 df,
                                 on=["obstime"],
                                 how="outer",
                                 suffixes=(None, f"_{file}"))

    # Separate time into date, and time separately

    df_merged["date"] = pd.to_datetime(df_merged["obstime"]).dt.strftime("%Y/%m/%d")
    df_merged["time"] = pd.to_datetime(df_merged["obstime"]).dt.strftime("%H:%M:%S")

    df_merged = df_merged[["date", "time"] + [col for col in df_merged.columns if col not in ["date", "time"]]]

    # Dropping the original datetime column
    # df_merged = df_merged.drop(columns=["obstime"])

    print(df_merged["Air Temperature in degree C"].value_counts().sum())

    return df_merged


def save_data(df):
    df.to_csv("processed-data/processed_2023_qc.csv")
    print("Save complete")


df_processed = process_qc()
save_data(df_processed)
