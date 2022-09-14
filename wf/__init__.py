"""
Fast Download GDC Data
"""
from datetime import datetime
from pathlib import Path
import subprocess
import os

from latch import small_task, workflow, message
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter, LatchDir



@small_task
def gdc_data_task(manifest: LatchFile) -> LatchDir:
    def get_timestamp():
        format_str = "%d %b %Y %H:%M:%S %p"
        result = datetime.now().strftime(format_str)
        return result
    
    curr_timestamp = "".join([x if x.isalnum() else "_" for x in get_timestamp()])
    out_dir = f"GDC_{curr_timestamp}"
    os.system(command=f"mkdir -p {out_dir}")
    
    message("info", {"title": f"Output Directory: {out_dir}", "body": f"Data will be downloaded to {out_dir} directory."})
    
    os.system(command=f"./gdc-client download -d {out_dir} -m {manifest.local_path}")
    return LatchDir(path=str(out_dir), remote_path=f"latch:///{out_dir}/")


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="GDC Data Transfer",
    documentation="https://gdc.cancer.gov/access-data/gdc-data-transfer-tool",
    author=LatchAuthor(
        name="Abdullah Al Nahid",
        email="abdnahid56@gmail.com",
        github="github.com/nahid18",
    ),
    repository="",
    license="MIT",
    parameters={
        "manifest": LatchParameter(
            display_name="GDC Manifest File",
            description="After adding products to the cart, go to cart, click on the 'Download' button and then select 'Manifest' to download the manifest file.",
            batch_table_column=False,
        )
    },
)


@workflow(metadata)
def gdc_data_transfer(manifest: LatchFile) -> LatchDir:
    """Fast Download GDC Data

    GDC Data Transfer
    ----
    [GDC Data Transfer Tool](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool) is a command-line tool supporting both GDC data downloads and submissions. Recommended for users with more command line experience that require large data transfers of GDC data or need to download a large numbers of data files.

    ## How to Access
    Click Here: 

    ## Requirement to Run
    1. *To download files:* Manifest file from [https://portal.gdc.cancer.gov](https://portal.gdc.cancer.gov) https://portal.gdc.cancer.gov after adding files to cart.

    **Note: `Currently only data download is available.`**
    """
    return gdc_data_task(manifest=manifest)


# Test Data
LaunchPlan(
    gdc_data_transfer,
    "Test Data",
    {
        "manifest": LatchFile("s3://latch-public/test-data/3729/manifest.txt"),
    },
)
