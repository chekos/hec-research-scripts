# When downloading data from the Compare Institutions option on their website (https://nces.ed.gov/ipeds/use-the-data) you get a zipped file with 2 csv files containing 1) the data and 2) value labels. This script helps you clean this data quickly into one working csv.

# -*- coding: utf-8 -*-
import click
import pandas as pd
from pathlib import Path
from zipfile import ZipFile
import re
from datetime import datetime as dt

today = dt.today().strftime("%Y-%m-%d")


@click.command()
@click.argument(
    "zipped_file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
)
@click.argument(
    "output_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    required=True,
)
def clean_data(zipped_file: str, output_dir: str):
    """Combines data and valuelabels csv files that come from IPEDS Compare Institutions option
    
    Parameters
    ----------
    zipped_file : str
        Name of zipped file
    output_dir : str
        Directory to which output combined CSV
    
    Returns
    -------
    None
        Combines files and outputs csv in `output_dir` directory.
    """
    OUTPUT_DIR = Path(output_dir)
    ZIPPED_FILE = ZipFile(zipped_file)
    DATA_FILE = [file for file in ZIPPED_FILE.namelist() if "Data" in file][0]
    VALUELABELS_FILE = [
        file for file in ZIPPED_FILE.namelist() if "ValueLabels" in file
    ][0]

    data = pd.read_csv(ZIPPED_FILE.open(DATA_FILE))
    value_labels = pd.read_csv(ZIPPED_FILE.open(VALUELABELS_FILE))

    # Clean empty column - Sometimes there's a "Unnamed" col in the data file
    data_cols = [col for col in data.columns if "Unnamed" not in col]
    data = data[data_cols].copy()

    # Adding value labels to dataframe
    labels = {}
    var_names = value_labels["VariableName"].unique()

    for var in var_names:
        mask = value_labels["VariableName"] == var
        working_df = value_labels[mask].copy()

        labels[var] = {}
        for row in working_df.itertuples():
            labels[var][row.Value] = row.ValueLabel

    for key in labels.keys():
        data[key] = data[key].map(labels[key])

    # Clean column names
    def clean_column(col: str) -> str:
        """Cleans column name from unwanted chars.
        
        Parameters
        ----------
        col : str
            Column name to clean
        
        Returns
        -------
        str
            Cleaned column name
        """
        col = (
            col.replace("'s", "")
            .replace(" - ", "_")
            .replace(" ", "_")
            .replace("/", "-")
            .replace("__", "_")
            .lower()
            # specifically for this context - after some iteration
            .replace("graduation_rate", "gradrate")
            .replace("_within_", "_")
            .replace("two_or_more_races", "multirace")
            .replace("_of_institution", "")
            .replace("_location", "")
            .replace("_or_post_office_box", "")
            .replace("_of_", "_")
            .replace("_that_are_", "_")
        )
        col = re.sub("_\([^()]*\)", "", col)

        return col

    data.columns = [clean_column(col) for col in data.columns]
    click.echo(click.style("Saving file", fg="green"))
    data.to_csv(
        OUTPUT_DIR / f"processed_data-{today}.csv", encoding="utf-8", index=False
    )


if __name__ == "__main__":
    clean_data()
