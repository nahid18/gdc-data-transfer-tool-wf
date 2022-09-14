"""
GDC Data Transfer Tool
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
    """GDC Data Transfer Tool

    GDC Data Transfer
    ----
    """
    return gdc_data_task(manifest=manifest)


# """
# Add test data with a LaunchPlan. Provide default values in a dictionary with
# the parameter names as the keys. These default values will be available under
# the 'Test Data' dropdown at console.latch.bio.
# """
LaunchPlan(
    gdc_data_transfer,
    "Test Data",
    {
        "manifest": LatchFile("s3://latch-public/test-data/3729/manifest.txt"),
    },
)
