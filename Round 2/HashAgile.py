import os
import pysolr
import pandas as pd
import csv

def createCollection(collection_name):
    command = f'sudo -u solr /opt/solr/bin/solr create -c {collection_name} -url http://localhost:8989'
    os.system(command)

def getEmpCount(collection_name):
    solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
    count=(solr.search("*:*").hits)
    print(f"Employee count : {count}")


def indexData(collection_name, exclude_column):    
    solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
    with open('./Employee Sample Data 1.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        documents = []
        for row in csv_reader:
            if exclude_column in row:
                del row[exclude_column]

                documents.append(row)

        if documents:
            solr.add(documents)
            print(f"indexed {len(documents)} records into '{collection_name}'.")

def index_data(collection_name, exclude_column):
    solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
    data = pd.read_csv('./Employee_Sample_Data_1.csv')

    if exclude_column in data.columns:
        df = df.drop(columns=[exclude_column])

    records = data.to_dict(orient="records")
    solr.add(records)


def searchByColumn(collection_name, p_column_name, p_column_value):

    try:
        solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
        query = f"{p_column_name}:{p_column_value}" if p_column_name and p_column_value else "*:*"
        results = solr.search(query)

        if len(results) > 0:
            print(f"Found {len(results)} record(s) in collection '{collection_name}':")
            for result in results:
                print(result)
            return list(results)
        else:
            print(f"No records found in collection '{collection_name}' for {p_column_name} = {p_column_value}.")
            return []

    except pysolr.SolrError as e:
        print(f"Solr error: {e}")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def delEmpById(collection_name, employee_id):
    solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
    solr.delete(id=employee_id)

def getDepFacet(collection_name):
    solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
    facet_query = solr.search("*:*", **{
        'facet': 'true',
        'facet.field': 'Department',
        'rows': 0
    })
    return facet_query.facets['facet_fields']['Department']


v_nameCollection = 'Hash_Sreehari'

v_phoneCollection = 'Hash_1543'

createCollection(v_nameCollection)

createCollection(v_phoneCollection)

getEmpCount(v_nameCollection)
print('\n')

indexData(v_nameCollection,'Department')
print('\n')

indexData(v_phoneCollection, 'Gender')
print('\n')

getEmpCount(v_nameCollection)
print('\n')

print("Deleting Employee whose id is E02003")
delEmpById(v_nameCollection, 'E02003')

getEmpCount(v_nameCollection)
print('\n')

print(f"SearchBY Column for collection : '{v_nameCollection}', 'Department', 'IT'")
searchByColumn(v_nameCollection, 'Department', 'IT')
print('\n')

print(f"SearchBY Column for collection : '{v_nameCollection}',  'Gender', 'Male'")
searchByColumn(v_nameCollection, 'Gender', 'Male')
print('\n')

print(f"SearchBY Column for collection : '{v_phoneCollection}', 'Department', 'IT'")
searchByColumn(v_phoneCollection, 'Department', 'IT')