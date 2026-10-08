"""
dashboard/cloud_app.py
----------------------
Entry point for the ONLINE copy of the dashboard (Streamlit Community Cloud).

Kafka and Spark can't run on a free web host, so this starts the normal
dashboard (dashboard/app.py) in replay mode: it plays back a recording of real
pipeline output saved in demo_data/ (made with dashboard/export_replay_data.py).

On Streamlit Community Cloud set "Main file path" to:  dashboard/cloud_app.py
"""

import os
import runpy

os.environ["DASHBOARD_MODE"] = "replay"
runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py"), run_name="__main__")
