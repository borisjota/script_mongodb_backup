# Python code to illustrate
# inserting data in MongoDB
from pymongo import MongoClient

# INFO FROM DB MIGRATE
url_migrate_db = 'URL_HOST_DB_ORIGIN'
port_migrate_db = 27017 #ESPECIFICAR PUERTO ORIGEN
migrate_data_bases_names = [] #Nombre o Nombres de BDs Origen

#INFO TO DB MIGRATE
to_url_db = 'URL_HOST_DB_DESTINATION'
to_port_db = 27017 #ESPECIFICAR PUERTO DESTINO
to_data_base_name = 'NEW_DB_NAME_DESTINATION' #Nuevo nombre de BD destino

try:
    migrate_conn = MongoClient(url_migrate_db, port_migrate_db)
    print("Connected successfully mongodb to origin!!!")
except:
    print("Could not connect to MongoDB")

try:
    to_conn = MongoClient(to_url_db, to_port_db)
    print("Connected successfully mongodb to destination!!!")
except:
    print("Could not connect to MongoDB")
new_db = to_conn[f"{to_data_base_name}"]


#BUCLE DATABASES
for db_name in migrate_data_bases_names:
    print('---------------> STARTING MIGRATION COLLECTIONS <----------------')
    db = migrate_conn[f"{db_name}"]
    list_name_collections = db.list_collection_names(filter={
        'type': 'collection', 'name': {'$ne': 'system.views'}
    })
    #BUCLE COLLECTIONS MIGRATE FOR DB
    for name_coll in list_name_collections:
        old_collection = db[f"{name_coll}"]
        cursor = old_collection.find()
        new_collection = new_db[f"{name_coll}"]
        new_db.create_collection(new_collection.name)
        for record in cursor:
            rec_id1 = new_collection.insert_one(record)
    print('---------------> FINISH MIGRATION COLLECTIONS <----------------')

    print('---------------> STARTING MIGRATION VIEWS <----------------')
    list_name_views = db.list_collection_names(filter={
        'type': 'view', 'name': {'$ne': 'system.views'}
    })
    #BUCLE VIEWS MIGRATE FOR DB
    for name_view in list_name_views:
        view = (db.command({'listCollections': 1,
                            'filter': {'name': name_view}
                            })['cursor']['firstBatch'][0]) #TODO mejorar esto
        new_db.command({
                'create': view['name'],
                "viewOn": view['options']['viewOn'],
                "pipeline": view['options']['pipeline']
            })
    print('---------------> FINISH MIGRATION VIEWS <----------------')


