from dagster import load_assets_from_package_module
from . import f1_predictor

TESTING = "testing"
testing_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=TESTING)