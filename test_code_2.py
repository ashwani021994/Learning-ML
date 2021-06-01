import requests
import json
from collections import namedtuple
from contextlib import closing
import sqlite3
from contextlib import closing
from prefect.tasks.database.sqlite import SQLiteScript
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import datetime
from prefect.engine import signals
# from prefect.engine.result_handlers import LocalResultHandler
import prefect
from prefect.storage import GitHub


# state handlers
def failed_alert(obj, old_state, new_state):
    if new_state.is_failed():
        print("Failed !!!!!!!!!!")


# setup##
create_table = SQLiteScript(db='cfpbcomplaints.db',
                            script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)')


# extract
@task(cache_for=datetime.timedelta(days=1), state_handlers=[failed_alert])
# caching this task's output for a day so it runs only once and rest time takes data stored in cached
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/",
                     params={'size': 10})
    response_json = json.loads(r.text)
    logger = prefect.context.get('logger')
    logger.info("i actually requetsed data this time!!!!!!")
    # server display official python logs and not prints statement on UI
    print("i actually requetsed data this time!!!!!!")
    raise signals.FAIL
    return response_json['hits']['hits']


# transform
@task(state_handlers=[failed_alert])
def parse_complaint_data(raw):
    complaints = []
    Complaint = namedtuple('Complaint', ['data_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source')
        this_complaint = Complaint(
            data_received=source.get('date_recieved'),
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints


# load
@task(state_handlers=[failed_alert])
def store_complaints(parsed):
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed)
            conn.commit()


# schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1)) # stopping schedule to test faster
# interval schedule here means flow will run in every 1 min ########

# we have state handler for flow as well so there are two types task state handlers and flow state handlers
with Flow("my etl flow", state_handlers=[failed_alert]) as f:
    db_table = create_table()
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)  # functional api (implicitly defining using output of one as input of other)
    populate_table = store_complaints(parsed)
    populate_table.set_upstream(db_table)  # imperative api (explicitly defining)

f.register(project_name="tutorial1")

f.storage = GitHub(
    repo="ashwani021994/Learning-ML",  # name of repo
    path="/test_code_2.py"

)


# Note:
# 1.) Trigger failed is subclass of failed class and it occurs when an upstream tasks fails
# 2.) we can explicitly make a task give final status as FAIL OR SUCCESS by
# using prefect signals instead the way we did above by raise exception
# 3.) LocalResultHandler places my result from the task in
# /prefect/results directory( i am not able to find prefect folder) data written is a pickle file
# 4.) Every time we call f.register a new version of flow is generated and older version appears in archive
# 5>) One another advantage of using cache on server is that the result handler writes data to database.
# If process dies in middle on local , cache is gone from memory , if similar happens on server ,
# cache can be retrieved from results on database.
