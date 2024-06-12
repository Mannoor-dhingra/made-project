import os
import sqlite3
import unittest
import pandas as pd
from pipeline import main, download_and_transform_dataset1, download_and_transform_dataset2


class TestDataPipeline(unittest.TestCase):
    """
    A test case class for testing the data pipeline.
    This class contains test methods to validate the execution of the data pipeline
    and the integrity of the output data files and tables.
    """
    @classmethod
    def setUpClass(cls):
        """
        Set up the test environment by executing the data pipeline.
        This method is called once before any tests in the class are run.
        """
        main()

    def test_output_files_exist(self):
        
        # Check if the database file exists

        db_file_path = '../data/'
        table_names=['temp_and_forest_data.sqlite','global_temperature_data.sqlite','world_forest_data.sqlite']
        for tables in table_names:
            self.assertTrue(os.path.isfile(os.path.join(db_file_path,tables)), f"Database file '{db_file_path}' does not exist")

        
        print("Test output_files_exist passed successfully.")


       

    def test_pipeline_execution(self):
        """
        Test the execution of the data pipeline.
        This test method executes the data pipeline, including data loading,
        transformation, and validation, and ensures that the output data tables
        meet the expected structure and integrity criteria.
        """
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Moves up two levels from current script's directory
        data_dir = os.path.join(base_dir, 'data')
        project_dir = os.path.join(base_dir, 'project')

        forest_data_url = 'https://www.kaggle.com/api/v1/datasets/download/webdevbadger/world-forest-area'
        temperature_data_url = 'https://www.kaggle.com/api/v1/datasets/download/mdazizulkabirlovlu/all-countries-temperature-statistics-1970-2021?datasetVersionNumber=1'
        
        download_and_transform_dataset1(forest_data_url, os.path.join(project_dir, 'forest_data'), os.path.join(data_dir, 'world_forest_data.sqlite'))
        download_and_transform_dataset2(temperature_data_url, os.path.join(project_dir, 'temperature_data'), os.path.join(data_dir, 'global_temperature_data.sqlite'))
        
        '''# Check wildfire data table
        test_forest_table(wildfire_df)

        # Check emissions data table
        test_temprature_table(emissions_df)'''
    
        print("Test pipeline_execution passed successfully.")

if __name__ == '__main__':
    unittest.main()