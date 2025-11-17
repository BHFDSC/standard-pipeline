import sys
import os
import pyspark.sql.functions as f
from pyspark.sql import Window
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from typing import Dict, Optional
import numpy as np
import math
from IPython.display import display
import matplotlib.image as mpimg
from .environment_utils import resolve_path


def plot_counts_by_batch(df,
                color_map=None,
                label_map=None,
                shade_on_drop=True,
                fig_width=14,
                fig_height=6,
                join_gaps=False,
                save_dir=None,
                dpi=150,
                file_type="png"):
    """
    Draw one figure per `table_name`, sharing a common x-axis of ALL
    `archived_on` dates.

    Parameters
    ----------
    df : DataFrame
        Columns required: archived_on, table_name, n, n_id, n_id_distinct
    color_map / label_map : dict, optional
        Override the default colours or legend labels.
    shade_on_drop : bool, default True
        Shade the final date block pink if ANY metric drops.
    fig_width / fig_height : numbers
        Matplotlib figure size in inches.
    join_gaps : bool, default False
        If True, connect non-contiguous points across missing dates with a
        **dotted** line (same colour as the metric’s solid line).
    """

    DEFAULT_COLOR_MAP = {
        "n": "#f5c542",
        "n_id": "#7e57c2",     
        "n_id_distinct": "#4db6ac"
        }

    DEFAULT_LABEL_MAP = {
        "n": "Records",
        "n_id": "Records with a de-identified PERSON ID",
        "n_id_distinct": "Distinct de-identified PERSON ID"
        }
    
    color_map = color_map or DEFAULT_COLOR_MAP
    label_map = label_map or DEFAULT_LABEL_MAP

    df = df.toPandas()
    df = df.copy()
    df["archived_on"] = pd.to_datetime(df["archived_on"], errors="coerce")
    df = df[df["archived_on"].notna()].sort_values("archived_on")

    if df.empty:
        raise ValueError("No valid archive dates to plot.")

    full_dates = pd.Index(df["archived_on"].unique(), name="archived_on")
    metrics = ["n", "n_id", "n_id_distinct"]


    for tbl, dft in df.groupby("table_name"):

        
        dft = (
            dft.set_index("archived_on")
               .reindex(full_dates)
               .sort_index()
        )

        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

       
        for x in full_dates:
            ax.axvline(x=x, color="grey", lw=0.5, alpha=0.2)

        
        for m in metrics:
            series = dft[m]

            ax.plot(series.index, series.values,
                    marker="o",
                    linewidth=2,
                    color=color_map.get(m),
                    label=label_map.get(m, m))

      
            if join_gaps:
                valid_pos = np.where(~series.isna())[0]
                for j in range(len(valid_pos) - 1):
                    i, k = valid_pos[j], valid_pos[j + 1]
                    if k - i > 1:
                        ax.plot(
                            [series.index[i], series.index[k]],
                            [series.iloc[i],  series.iloc[k]],
                            linestyle="dotted",
                            linewidth=2,
                            color=color_map.get(m)
                        )

        
        valid_rows = dft.dropna(subset=metrics)
        if shade_on_drop and len(valid_rows) >= 2:
            prev, last = valid_rows.iloc[-2], valid_rows.iloc[-1]
            if (last[metrics] < prev[metrics]).any():
                ax.axvspan(prev.name, last.name, color="pink", alpha=0.85)

      
        ax.set_xticks(full_dates)
        ax.set_xticklabels(full_dates.strftime("%Y-%m-%d"),
                           rotation=90,
                           ha="center",
                           va="top")
        ax.tick_params(axis="x", labeltop=False) 

        ax.set_xlabel("Archive Date")
        ax.set_ylabel("Count")
        ax.set_title(f"Count over Time: {tbl}", weight="bold")
        ax.legend(loc="upper left", frameon=False)


        fig.subplots_adjust(bottom=0.25)
        plt.tight_layout()
        

        if save_dir:
            fname = f"{tbl}.{file_type}"
            fig.savefig(os.path.join(save_dir, fname),
                        dpi=dpi, format=file_type, bbox_inches="tight")
        else:
            plt.show()

        plt.close(fig)



def display_all_pngs(folder_path, figsize=(20, 20)):
    """
    Displays all PNG images in a folder.

    Parameters:
    - folder_path (str): Top-level DBFS path
    - figsize (tuple): Figure size for each image
    """
  
    folder_path = folder_path
    files = [f for f in os.listdir(f"{folder_path}") if f.endswith(".png")]

    for filename in sorted(files):
        full_path = f"{folder_path}/{filename}"
        img = mpimg.imread(full_path)

        plt.figure(figsize=(20,20))
        plt.imshow(img)
        plt.axis('off')
        plt.title(filename)
        plt.show()


def display_png(file_path, figsize=(20, 20)):
    """
    Displays PNG image.

    """
    full_path = f"{file_path}.png"
    img = mpimg.imread(full_path)

    plt.figure(figsize=(20,20))
    plt.imshow(img)
    plt.axis('off')
    plt.show()



def overall_batch_changes(df):

    w = Window.partitionBy("table_name").orderBy("archived_on")

    return(df
    .withColumn("date_previous", f.lag("archived_on").over(w))
    .withColumn("count_prev_n", f.lag("n").over(w))
    .withColumn("count_prev_n_id", f.lag("n_id").over(w))
    .withColumn("count_prev_n_dist", f.lag("n_id_distinct").over(w))
    .withColumn("metrics", f.array(
        f.struct(
            f.lit("n").alias("record_type"),
            f.col("n").alias("count_last"),
            f.col("count_prev_n").alias("count_previous"),
            f.col("archived_on").alias("date_last"),
            f.col("date_previous")
        ),
        f.struct(
            f.lit("n_id").alias("record_type"),
            f.col("n_id").alias("count_last"),
            f.col("count_prev_n_id").alias("count_previous"),
            f.col("archived_on").alias("date_last"),
            f.col("date_previous")
        ),
        f.struct(
            f.lit("n_id_distinct").alias("record_type"),
            f.col("n_id_distinct").alias("count_last"),
            f.col("count_prev_n_dist").alias("count_previous"),
            f.col("archived_on").alias("date_last"),
            f.col("date_previous")
        )
    ))
    .select("table_name", f.explode("metrics").alias("m"))
    .select(
        "table_name",
        f.col("m.record_type"),
        f.col("m.date_previous"),
        f.col("m.date_last"),
        f.col("m.count_previous"),
        f.col("m.count_last"),
        f.when(
            f.col("m.count_previous").isNotNull() & (f.col("m.count_previous") != 0),
            f.round(((f.col("m.count_last") / f.col("m.count_previous")) - 1) * 100, 2)
        ).alias("pct_change")
    )
)

def write_strings_to_txt(df, output_path):
    df = (df.withColumn("row_string",f.concat_ws(" | ", *df)))
    rows = df.select("row_string").rdd.map(lambda r: r["row_string"]).collect()
    header = " | ".join(df.columns)
    with open(output_path, "w") as file_out:
        file_out.write(header + "\n")
        for row in rows:
            file_out.write(row + "\n")


import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm

def plot_coverage_by_archive(
    df,
    value_col="n",
    n_archived=5,
    fig_width=14,
    fig_height=6,
    filter_year_min = 1990,
    filter_year_max = 2030,
    highlight_most_recent="no",
    save_dir=None,
    dpi=150,
    file_type="png"):

    df=(df
        .filter(f.col("date_ym").isNotNull()).filter(f.col("archived_on").isNotNull())
        .withColumn("date_ym_date", f.to_date(f.concat_ws("-", f.col("date_ym"), f.lit("01"))))
        .filter(
            (f.col("date_ym_date") >= f.lit(f"{filter_year_min}-01-01")) & 
            (f.col("date_ym_date") <= f.lit(f"{filter_year_max}-12-31"))
            )
        .drop("date_ym_date")
    )
    if n_archived != "all":
        archived_pairs = df.select("table_name", "archived_on").dropDuplicates()
        window_spec = Window.partitionBy("table_name").orderBy(f.col("archived_on").desc())
        archived_pairs = archived_pairs.withColumn("row_num", f.row_number().over(window_spec))
        archived_pairs = archived_pairs.filter(f.col("row_num") <= n_archived).drop("row_num")
        df = df.join(archived_pairs, on=["table_name", "archived_on"], how="inner")

    df=df.toPandas()
    df = df.dropna(subset=["date_ym", "archived_on"]).copy()
    df["date_ym"] = pd.to_datetime(df["date_ym"].astype(str) + "-01")
    df["archived_on"] = pd.to_datetime(df["archived_on"])

    unique_archives = sorted(df["archived_on"].dropna().unique())
    cmap = cm.get_cmap("tab10", len(unique_archives))
    grey_colour = "#A9A9A9"
    highlight_colours = {
        "n": "#f5c542",
        "n_id": "#7e57c2",
        "n_id_distinct": "#4db6ac"
    }
 
    for table in sorted(df["table_name"].unique()):
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        subset = df[df["table_name"] == table]
        max_archived_date = subset["archived_on"].max()
        if highlight_most_recent in ["Yes", "yes", 1]:
            color_map = {archived_date: (highlight_colours.get(value_col, "#000000") if archived_date == max_archived_date else grey_colour)  for archived_date in unique_archives}
            style_map = {archived_date: ('-' if archived_date == max_archived_date else '--') for archived_date in unique_archives}
        else:
            color_map = {archived_date: cmap(i) for i, archived_date in enumerate(unique_archives)}
            style_map = {archived_date: '-' for archived_date in unique_archives}
            

        for archived_date in unique_archives:
            archived_subset = subset[subset["archived_on"] == archived_date].sort_values("date_ym")
            if not archived_subset.empty:
                label = f"Archived {pd.Timestamp(archived_date).strftime('%Y-%m-%d')}"
                ax.plot(
                    archived_subset["date_ym"],
                    archived_subset[value_col],
                    label=label,
                    color=color_map[archived_date],
                    linestyle=style_map[archived_date]
                )


        title_map = {
            "n": "Records",
            "n_id": "Records with a de-identified PERSON ID",
            "n_id_distinct": "Distinct de-identified PERSON ID"
            }
        final_title = title_map.get(value_col, value_col)
        if n_archived == "all":
            archive_prefix = "for all archived dates"
        else:
            archive_prefix = f"for last {n_archived} archived dates"
        ax.set_title(f"Coverage ({final_title}) {archive_prefix}: {table}", weight="bold")
        ax.set_xlabel("Date")
        ax.set_ylabel(f"Coverage ({value_col})")
        ax.grid(True)
        for label in ax.get_xticklabels():
            label.set_rotation(45)

  
        # plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
        plt.legend(loc="upper left", frameon=False)
        plt.tight_layout(rect=[0, 0, 0.85, 1]) 
        
        fig.subplots_adjust(bottom=0.25)
        plt.tight_layout()
        if save_dir:
            fname = f"{table}_{value_col}.{file_type}"
            fig.savefig(os.path.join(save_dir, fname),
                        dpi=dpi, format=file_type, bbox_inches="tight")
        else:
            plt.show()

        plt.close(fig)



import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def plot_coverage_by_records(
    df,
    archived_on_date="latest",
    metrics=("n", "n_id", "n_id_distinct"),
    fig_width=14,
    fig_height=6,
    filter_year_min = 1990,
    filter_year_max = 2030,
    save_dir=None,
    dpi=150,
    file_type="png"):

    df=(df
        .filter(f.col("date_ym").isNotNull()).filter(f.col("archived_on").isNotNull())
        .withColumn("date_ym_date", f.to_date(f.concat_ws("-", f.col("date_ym"), f.lit("01"))))
        .filter(
            (f.col("date_ym_date") >= f.lit(f"{filter_year_min}-01-01")) & 
            (f.col("date_ym_date") <= f.lit(f"{filter_year_max}-12-31"))
            )
        .drop("date_ym_date")
    )
    
    if archived_on_date=="latest":
        window_spec = Window.partitionBy("table_name")
        df = (
            df.withColumn("max_archived_on", f.max("archived_on").over(window_spec))
            .filter(f.col("archived_on") == f.col("max_archived_on")).drop("max_archived_on")
        )
    else :
        df = (df.filter(f.col("archived_on") == f.lit(archived_on_date)))
        archived_on_date = pd.to_datetime(archived_on_date)

    df=df.toPandas()
    df["date_ym"] = pd.to_datetime(df["date_ym"].astype(str) + "-01", errors="coerce")
    df["archived_on"] = pd.to_datetime(df["archived_on"], errors="coerce")
    df = df.dropna(subset=["date_ym", "archived_on"])


    color_map = {
        "n": "#f5c542",
        "n_id": "#7e57c2",     
        "n_id_distinct": "#4db6ac"
    }

    label_map = {
        "n": "Records",
        "n_id": "Records with a de-identified PERSON ID",
        "n_id_distinct": "Distinct de-identified PERSON ID"
    }

    table_names = sorted(df["table_name"].unique())
    n_tables = len(table_names)

    n_cols = 1
    n_rows = math.ceil(n_tables / n_cols)

 
    for i, table in enumerate(table_names):
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        subset = df[df["table_name"] == table].sort_values("date_ym")

        for metric in metrics:
            if metric in subset.columns:
                ax.plot(
                    subset["date_ym"],
                    subset[metric],
                    label=label_map.get(metric, metric),
                    color=color_map.get(metric, "black")
                )

        if archived_on_date=="latest":
            latest_date = pd.to_datetime(subset["archived_on"].unique()).max().date()
            ax.set_title(f"Coverage for latest archive {latest_date}: {table}", weight="bold")
        else:
            date = pd.to_datetime(archived_on_date).date()
            ax.set_title(f"Coverage for archive {date}: {table}", weight="bold")
        ax.set_xlabel("Date")
        ax.set_ylabel("Monthly count")
        ax.grid(True)
        ax.tick_params(axis='x', rotation=45)
        # ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
        ax.legend(loc="upper left", frameon=False)


        if save_dir:
            fname = f"{table}.{file_type}"
            fig.savefig(os.path.join(save_dir, fname),
                        dpi=dpi, format=file_type, bbox_inches="tight")
        else:
            plt.show()

        plt.close(fig)


def detect_coverage_lag(subset, metric_col, window=5):
    """

    """
    subset = subset.copy()
    subset['rolling_mean'] = subset[metric_col].rolling(window=window, min_periods=1).mean()
    subset['drop_pct'] = 1 - (subset[metric_col] / subset['rolling_mean'])

    subset = subset.sort_values("date_ym", ascending=False).reset_index(drop=True)
    for i in range(len(subset)):
        if subset['drop_pct'].iloc[i] < 0:
            return subset["date_ym"].iloc[i]
    return None


import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def plot_lag(
    df,
    archived_on_date="latest",
    metrics="n",
    window=3,
    recent_window=4,
    drop_threshold=0.2,
    no_years_to_plot=5,
    rolling_color="#EC217D",
    fig_width=14,
    fig_height=6,
    filter_year_min = 1990,
    filter_year_max = 2030,
    save_dir=None,
    dpi=150,
    file_type="png"):

    df=(df
        .filter(f.col("date_ym").isNotNull()).filter(f.col("archived_on").isNotNull())
        .withColumn("date_ym_date", f.to_date(f.concat_ws("-", f.col("date_ym"), f.lit("01"))))
        .filter(
            (f.col("date_ym_date") >= f.lit(f"{filter_year_min}-01-01")) & 
            (f.col("date_ym_date") <= f.lit(f"{filter_year_max}-12-31"))
            )
    )

    if archived_on_date=="latest":
        window_spec = Window.partitionBy("table_name")
        df = (
            df.withColumn("max_archived_on", f.max("archived_on").over(window_spec))
            .filter(f.col("archived_on") == f.col("max_archived_on")).drop("max_archived_on")
        )
    else :
        df = (df.filter(f.col("archived_on") == f.lit(archived_on_date)))
        archived_on_date = pd.to_datetime(archived_on_date)

    latest_dates = (df.groupBy("table_name").agg(f.max("date_ym_date").alias("latest_date")))
    df = df.join(latest_dates, on="table_name", how="left")
    df = (df.filter(f.col("date_ym_date") >= f.date_sub(f.col("latest_date"), 365*no_years_to_plot)).drop("date_ym_date","latest_date"))
    

    df=df.toPandas()
    df["date_ym"] = pd.to_datetime(df["date_ym"].astype(str) + "-01", errors="coerce")
    df["archived_on"] = pd.to_datetime(df["archived_on"], errors="coerce")
    df = df.dropna(subset=["date_ym", "archived_on"])


    color_map = {
        "n": "#f5c542",
        "n_id": "#7e57c2",     
        "n_id_distinct": "#4db6ac"
    }

    label_map = {
        "n": "Records",
        "n_id": "Records with a de-identified PERSON ID",
        "n_id_distinct": "Distinct de-identified PERSON ID"
    }

    table_names = sorted(df["table_name"].unique())
    n_tables = len(table_names)

    n_cols = 1
    n_rows = math.ceil(n_tables / n_cols)

 
    for i, table in enumerate(table_names):
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        subset = df[df["table_name"] == table].sort_values("date_ym")
        subset["rolling_avg"] = subset[metrics].rolling(window=window, min_periods=1).mean()

        lag_date = detect_coverage_lag(subset, metrics, window=window)

        # Plot

        ax.plot(
            subset["date_ym"],
            subset["rolling_avg"],
            label=f"Rolling Avg ({window} months)",
            linestyle='--',
            color=rolling_color
        )

        ax.plot(
                    subset["date_ym"],
                    subset[metrics],
                    label=label_map.get(metrics, metrics),
                    color=color_map.get(metrics, "black"),
                    marker='o',
                )
        
        if lag_date:
            ax.axvline(pd.to_datetime(lag_date), color="red", linestyle=":", linewidth=2)
            ax.text(pd.to_datetime(lag_date) - pd.Timedelta(days=10),
                     ax.get_ylim()[1]*0.95, 
                    lag_date.strftime('%Y-%m'), color="red", rotation=90, va="top", ha="center")

        title_map = {
            "n": "Records",
            "n_id": "Records with a de-identified PERSON_ID",
            "n_id_distinct": "Distinct de-identified PERSON_ID"
            }
        final_title = title_map.get(metrics, metrics)
        if archived_on_date=="latest":
            latest_date = pd.to_datetime(subset["archived_on"].unique()).max().date()
            ax.set_title(f"Coverage ({final_title}) of the most recent {no_years_to_plot} years for latest archive {latest_date}: {table}", weight="bold")
        else:
            date = pd.to_datetime(archived_on_date).date()
            ax.set_title(f"Coverage ({final_title}) of the most recent {no_years_to_plot} years for archive {date}: {table}", weight="bold")
        ax.set_xlabel("Date")
        ax.set_ylabel("Monthly count")
        ax.grid(True)
        ax.tick_params(axis='x', rotation=45)
        # ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
        ax.legend(loc="upper left", frameon=False)


        if save_dir:
            fname = f"{table}.{file_type}"
            fig.savefig(os.path.join(save_dir, fname),
                        dpi=dpi, format=file_type, bbox_inches="tight")
        else:
            plt.show()

        plt.close(fig)



from IPython.display import display, HTML
def data_quality_report(table_name_list, outputs_folder, figsize=(20, 20), include_text=True):
  display(HTML(f'<div style="font-size:30px; font-weight:bold; color:black;">Data Quality Report for {", ".join(table_name_list)}<br><br></div>'))
  display(HTML(f'<div style="font-size:20px; font-weight:bold; color:black;">Archive (Batch) Check</div>'))
  if include_text:
    display(HTML("""
    <ul style="font-size:16px; line-height:1.6;color:#6D7074">
      <li><strong>What this plot shows:</strong><br>
        This line plot displays cumulative record counts over time, with the y-axis showing counts and the x-axis showing archive dates.
        Use this plot to check that the data is increasing over time as expected – each new archive should add more records.
      </li>
      <li><strong>X-axis includes all archive dates:</strong><br>
        The archive dates on the x-axis include all batches from across every dataset,
        so there may be gaps in the plotted line if a particular dataset wasn’t updated in a given batch.
      </li>
      <li><strong>Red shading highlights potential data issues:</strong><br>
        If there’s a decrease in any of the count types from the second-most-recent archive to the most recent one, that drop is highlighted in red.
        This is a flag for possible missing or incomplete data in the latest batch.
      </li>
      <li><strong>Don’t assume the most recent archive is safest:</strong><br>
        If there’s a drop in the most recent archive, it may not yet reflect complete or reliable data.
        In that case, consider using the most recent stable archive instead – one that shows no decrease in counts.
      </li>
      <li><strong>Check alignment across record types:</strong><br>
        All three lines (e.g., total records, records with ID, distinct IDs) should generally trend together.
        If one metric diverges or drops independently, this might indicate issues with data linkage.
      </li>
    </ul>
    """))
  for name in table_name_list:
      file_path = f"{outputs_folder}/batch_checks/plots/{name}"
      display_png(file_path=file_path, figsize=figsize)

  display(HTML(f'<div style="font-size:20px; font-weight:bold; color:black;">Archive Coverage Consistency</div>'))
  if include_text:
    display(HTML("""
    <ul style="font-size:16px; line-height:1.6; color: #6D7074">
      <li><strong>What these plots show:</strong><br>
        These plots compare coverage over time for the most recent data archive (shown in colour) against the four previous archives (shown in grey).
      </li>
      <li><strong>What these plots focus on:</strong><br>
        These plots help assess whether the most recent archive contains consistent coverage across the full time range — especially compared to previous versions.
        Even if the total number of records is higher overall, records could still be missing for specific time periods.
        It’s important to check that the general pattern and shape of the line are similar across archives, and that records haven’t been lost or shifted to different dates.
      </li>
      <li><strong>The most recent archive should provide the most up-to-date coverage:</strong><br>
        Because each new archive includes additional recent data, the coloured line should typically extend to later dates than the previous (grey) lines.
      </li>
    </ul>
    """))
  for name in table_name_list:
      file_path = f"{outputs_folder}/coverage_by_archive/plots/{name}_n"
      display_png(file_path=file_path, figsize=figsize)
      file_path = f"{outputs_folder}/coverage_by_archive/plots/{name}_n_id"
      display_png(file_path=file_path, figsize=figsize)
      file_path = f"{outputs_folder}/coverage_by_archive/plots/{name}_n_id_distinct"
      display_png(file_path=file_path, figsize=figsize)


  display(HTML(f'<div style="font-size:20px; font-weight:bold; color:black;">Consistency Across Record Types</div>'))
  if include_text:
    display(HTML("""
    <ul style="font-size:16px; line-height:1.6; color: #6D7074">
      <li><strong>What this plot shows:</strong><br>
        This plot displays trends over calendar time for all three record types — total records, records with a de-identified PERSON ID, and distinct de-identified PERSON IDs — using only the most recent archive.
        All lines represent counts over time within the latest available dataset.
      </li>
      <li><strong>What to look for:</strong><br>
        The lines should generally follow the same pattern over time.
        If one line diverges — for example, starts later, ends earlier, or shows a noticeably different shape — it may indicate an issue.
        For instance, if the PERSON ID line drops while the total records line remains stable, this could suggest that PERSON IDs are missing or not properly linked during that period.
      </li>
    </ul>
    """))
  for name in table_name_list:
      file_path = f"{outputs_folder}/coverage_by_records/plots/{name}"
      display_png(file_path=file_path, figsize=figsize)


  display(HTML(f'<div style="font-size:20px; font-weight:bold; color:black;">Lag Detection</div>'))
  if include_text:
    display(HTML("""
    <ul style="font-size:16px; line-height:1.6; color: #6D7074">
      <li><strong>What this plot shows:</strong><br>
        This plot shows monthly record counts over time for the most recent archive, with a focus on the last 5 years to highlight recent patterns.
        A rolling average line (pink) has been added to smooth short-term fluctuations and help visualise the overall trend more clearly.
      </li>
      <li><strong>Why lag occurs:</strong><br>
        Coverage for many datasets does not typically extend right up to the archive date. This is due to natural delays in data submission and processing.
        As a result, the most recent 3–4 months of data often appear to decline, but this is usually due to lag — not a real drop in activity.
      </li>
      <li><strong>How lag is highlighted in this plot:</strong><br>
        A red vertical line has been added to indicate the last month before a potential drop in data is detected — based on a simple rolling average comparison.
        The red line is generated by checking when the latest values drop below a recent rolling average.
        It marks the last point (going backwards in time) before a negative drop is detected.
        This is a helpful guide, but may not always align with the true data lag.
        Visual inspection is encouraged — especially when the dataset has seasonal variation or irregular data flow.
      </li>
      <li><strong>Best practice:</strong><br>
        Use this plot to help determine an appropriate study end date based on data completeness.
        In many datasets, excluding the most recent 3–4 months helps ensure analyses are based on stable and complete records.
        However, for some datasets with minimal or no lag, it may be appropriate for the study end date to align with the archive date.
      </li>
    </ul>
    """))

  for name in table_name_list:
      file_path = f"{outputs_folder}/lag/plots/{name}"
      display_png(file_path=file_path, figsize=figsize)



from datetime import datetime

def get_latest_archive(date_list):
    """
    Returns the latest date from a list of date strings formatted as 'YYYY_MM_DD'.
    
    Parameters:
        date_list (list): A list of strings representing dates in 'YYYY_MM_DD' format.
    
    Returns:
        str: The latest date as a string in 'YYYY_MM_DD' format.
    """
    date_objs = [datetime.strptime(d, "%Y_%m_%d") for d in date_list]
    latest_date = max(date_objs)
    return latest_date.strftime("%Y_%m_%d")