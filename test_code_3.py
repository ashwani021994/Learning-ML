from prefect import task, Flow
from prefect.storage import github

@task
def get_data():
    return [1, 2, 3, 4, 5]

@task
def print_data(data):
    print(data)

with Flow("example") as flow:
    data = get_data()
    print_data(data)

flow.storage = Github(
    repo="Learning-ML",                            # name of repo
    path="/test_code_3.py",

)



