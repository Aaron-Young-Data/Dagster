import os

class FileUtils:
    @staticmethod
    def file_to_query(filename):
        dagster_home = 'dagster_project'
        filename = filename + '.sql'

        for root, dirs, files in os.walk(dagster_home + '/scripts/'):
            if filename in files:
                with open(root + '/' + filename, 'r') as file:
                    query = file.read()
                return query
