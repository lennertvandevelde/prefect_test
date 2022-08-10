from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
import fsspec


import requests
from requests.auth import HTTPBasicAuth
import json
import csv
from prefect.blocks.system import Secret


# from prefect_dask.task_runners import DaskTaskRunner

def ie_classification(record):
    # print(record["Administrative"]["Type"])
    # print(len(record["Administrative"]["ExternalId"]))
    # print(record["Administrative"]["ExternalId"])
    if record["Administrative"]["Type"].lower() in ["audiofragment", "videofragment"]:
        return False
    elif not record["Administrative"]["ExternalId"]:
        print(f"ERROR: {record['Dynamic']['PID']}")
        return False
    elif len(record["Administrative"]["ExternalId"]) == 10:
        return True
    else:
        return False

@task
def write_record(record):
    if ie_classification(record):
        row = {}
        for field in fieldnames:
            for key in record:
                if field in record[key]:
                    # print(field)
                    # print(record[key][field])
                    if record[key][field]:
                        if type(record[key][field]) is str:
                            row.update({field:   str(record[key][field]).replace("\n", "")})
                        elif type(record[key][field]) is list:
                            row.update({field: str(record[key][field]).replace("\n", "").replace(",", ";")})
                        elif type(record[key][field]) is dict:
                            value = ""
                            for key2 in record[key][field]:
                                value +=  (key2 + ":" + str(record[key][field][key2])).replace("\n", "").replace(",", ";").replace("[", "").replace("]", "") +" "
                            row.update({field: value})
                        # row.update({field: str(record[key][field]).replace("\n", "").replace(",", ";")})
        # print(row)
        writer.writerow(row)
    #     return row
    # else:
    #     return None

@task
def make_api_call(start_index):
        url = "https://archief.viaa.be/mediahaven-rest-api/resources/media"

        querystring = {"q":"+(CP_id:OR-8s4jn9m)", "startIndex": start_index}

        payload = ""
        headers = {
            "Accept": "application/vnd.mediahaven.v2+json"
        }

        response = requests.request("GET", url, data=payload, headers=headers, params=querystring, verify=False, auth=HTTPBasicAuth('viaa@viaa', secret_block.get()))
        
        # print(response.text)
        parsed = json.loads(response.text)
        # print(parsed)
        return parsed
        
@flow(name="export_flow")
def export_flow():
    start_index = 0
    total_nr_of_results = float("inf")
    parsed = make_api_call.submit(start_index)
    write_record.map(parsed.result()["MediaDataList"])
    total_nr_of_results = parsed.result()["TotalNrOfResults"]
    max_start_index = int(total_nr_of_results / 25)
    print(max_start_index)
    # while start_index < total_nr_of_results:
        # if start_index == 100:
        #     break
    start_indeces = range(25, max_start_index*25+25, 125)
    for i in start_indeces:
        parseds = make_api_call.map(range(i, i+125, 25))
        # print(parsed)
        
        
        # print(json.dumps(parsed, indent=4, sort_keys=True))

        # print(parsed["MediaDataList"])

        # for record in parsed.result()["MediaDataList"]:
        for api_call_result in parseds:
            write_record.map(api_call_result.result()["MediaDataList"])
        #     if row:
        #         rows.append(row.result())
        # for row in rows:
        #     writer.writerow(row)


print("HI")

csvfile=open('test.csv', 'w')
fieldnames = ["PID", "FragmentId", "Title", "Description", "dc_identifier_localid", "dc_rights_licenses", "dc_identifier_localids"]
writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
writer.writeheader()

secret_block = Secret.load("mhapi-prd-viaa-password")



if __name__ == '__main__':

    export_flow()

csvfile.close()
