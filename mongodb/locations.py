import os
import sys
from mongodb.connect import connect_mongodb, get_collection
import pandas as pd
from pandas import DataFrame

COLLECTION_NAME = "location"
