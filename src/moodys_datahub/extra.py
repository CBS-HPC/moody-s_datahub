from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
import polars as pl
from rapidfuzz import process


# Common Wrangling Functions
def year_distribution(df=None):
    """Print year-frequency and percentage distribution from a date column."""

    if df is None:
        print("No Dataframe (df) was detected")
        return

    columns_to_check = ["closing_date", "information_date"]
    date_col = next((col for col in columns_to_check if col in df.columns), None)

    if not date_col:
        print("No valid date columns found")
        return

    # Convert the date column to datetime
    df[date_col] = pd.to_datetime(df[date_col], format="%d-%m-%Y")

    # Create a new column extracting the year
    df["year"] = df[date_col].dt.year

    year_counts = df["year"].value_counts().reset_index()
    year_counts.columns = ["Year", "Frequency"]

    # Sort by year
    year_counts = year_counts.sort_values(by="Year")

    # Calculate percentage
    year_counts["Percentage"] = (
        year_counts["Frequency"] / year_counts["Frequency"].sum()
    ) * 100

    # Calculate total row
    total_row = pd.DataFrame(
        {
            "Year": ["Total"],
            "Frequency": [year_counts["Frequency"].sum()],
            "Percentage": [year_counts["Percentage"].sum()],
        }
    )

    # Concatenate total row to the DataFrame
    year_counts = pd.concat([year_counts, total_row])

    # Display the table
    print(year_counts)


def national_identifer(obj, national_ids: list = None, num_workers: int = -1):
    """Return matching rows for the provided national IDs."""
    new_obj = obj.copy_obj()
    new_obj.set_data_product = "Key Financials (Monthly)"
    new_obj.set_table = "key_financials_eur"

    select_cols = ["bvd_id_number", "national_id_number"]
    query = pl.col("national_id_number").cast(pl.Utf8, strict=False).is_in(
        [str(item) for item in national_ids]
    )

    # Execute
    df, _ = new_obj.process_all(
        num_workers=num_workers,
        select_cols=select_cols,
        query=query,
    )

    return df


def _fuzzy_worker(args):
    """Execute fuzzy matching for one worker chunk."""
    names, cut_off, df_chunk, match_column, return_column, remove_str = args
    results = []

    # Create the choices list from the df_chunk based on match_column
    choices_chunk = [choice.lower() for choice in df_chunk[match_column].tolist()]

    # Function to remove substrings from choices if specified
    def remove_substrings(choices, substrings):
        for substring in substrings:
            choices = [choice.replace(substring.lower(), "") for choice in choices]
        return choices

    if remove_str:
        choices_chunk = remove_substrings(choices_chunk, remove_str)

    # Create a mapping of choice to index for fast exact match lookup
    choice_to_index = {choice: i for i, choice in enumerate(choices_chunk)}

    for name in names:
        # First, check if an exact match exists in the choices chunk
        if name in choice_to_index:
            match_index = choice_to_index[name]
            match_value = df_chunk.iloc[match_index][match_column]
            return_value = df_chunk.iloc[match_index][return_column]
            results.append(
                (name, name, 100, match_value, return_value)
            )  # Exact match with score 100
        else:
            # Perform fuzzy matching if no exact match is found
            match_obj = process.extractOne(name, choices_chunk, score_cutoff=cut_off)
            if match_obj:
                match, score, match_index = match_obj
                match_value = df_chunk.iloc[match_index][match_column]
                return_value = df_chunk.iloc[match_index][return_column]
                results.append((name, match, score, match_value, return_value))
            else:
                results.append((name, None, 0, None, None))

    return results


def fuzzy_match(
    df: pd.DataFrame,
    names: list,
    match_column: str = None,
    return_column: str = None,
    cut_off: int = 50,
    remove_str: list = None,
    num_workers: int = None,
):
    """Return best fuzzy matches for `names` against `match_column`."""

    names = [name.lower() for name in names]

    # Determine the number of workers if not specified
    if not num_workers or num_workers < 0:
        num_workers = max(1, cpu_count() - 2)

    # Ensure number of workers is not greater than the DataFrame size
    if len(df) < num_workers:
        num_workers = len(df)

    # Parallel processing
    matches = []
    if num_workers > 1:
        # Split the DataFrame according to the number of workers
        df_chunks = np.array_split(df, num_workers)

        # Prepare argument list for each worker (each gets the full names list and its own df_chunk)
        args_list = [
            (names, cut_off, df_chunk, match_column, return_column, remove_str)
            for df_chunk in df_chunks
        ]

        with Pool(processes=num_workers) as pool:
            results = pool.map(_fuzzy_worker, args_list)
            for result_batch in results:
                matches.extend(result_batch)
    else:
        matches.extend(
            _fuzzy_worker((names, cut_off, df, match_column, return_column, remove_str))
        )

    # Create the result DataFrame
    result_df = pd.DataFrame(
        matches,
        columns=["Search_string", "BestMatch", "Score", match_column, return_column],
    )

    # Group by 'Search_string' and get the highest score matches
    max_scores = result_df.groupby("Search_string", as_index=False)["Score"].max()
    best_matches = pd.merge(result_df, max_scores, on=["Search_string", "Score"])

    # Keep only unique rows
    unique_best_matches = best_matches.drop_duplicates()

    return unique_best_matches.reset_index(drop=True)
