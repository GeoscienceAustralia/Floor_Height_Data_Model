from fastapi import FastAPI

from floorheights.datamodel.models import AddressPoint

description = """
The Floor Heights API provides access to the Floor Heights
Data model through a series of rest endpoints.

## Acknowledgements
Developed by FrontierSI (https://frontiersi.com.au/)
"""

app = FastAPI(
    title="Floor Heights API",
    description=description,
    version='0.0.1',
)

@app.get("/api/")
def read_root():
    return {"Hello": "Floor Heights API"}

