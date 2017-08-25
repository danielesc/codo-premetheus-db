# -*- coding: utf-8 -*-

import pprint
import re
import csv
import pymongo

from bson.objectid import ObjectId

COORD = "Coordinación"
PLAN = "Plan"
CLAVE = "Cve."
MATERIA = "Materia"
HORA = "Hora"
FREC = "Frec"
DIAS = "Días"
HORAS = "Horas"
GRUPO = "Gpo."
HORA = "Hr."
DIA = "Día"
SALON = "Salón"
EMPLEADO = "Empleado"
NOMBRE = "Nombre"
LIMITE = "Límite"
NEXUS = "X"
LAB = "LAB"
PERIODO = "PERIODO"

client = pymongo.MongoClient('localhost', 27017)
db = client.codo2

def create_rooms_db(_arr_obj):
    _collection = db.Room
    result = _collection.insert_many(_arr_obj)
    print (result.inserted_ids)
    print ('insertados: ' + str(_collection.count()))

    ## set index in room => 4014, 2009, etc...
    result = _collection.create_index([('room', pymongo.ASCENDING)], unique=True)
    print (sorted(list(_collection.index_information())))

def create_assigments_db(_arr_obj):
    _collection = db.Assigment
    result = _collection.insert_many(_arr_obj)
    print (result.inserted_ids)
    print ('insertados: ' + str(_collection.count()))

    result = _collection.create_index([('rawName', pymongo.ASCENDING)], unique=False)
    print (sorted(list(_collection.index_information())))

def professor_by_id():
    _collection = db.Professor
    cursor = _collection.find_one({'_id': ObjectId('5959b74cf225183972411d81') })
    for assigment in cursor['assigments']:
        cursor = db.Assigment.find_one({'_id': ObjectId(assigment) })
        pprint.pprint(cursor)

def create_professors_db(_arr_obj):
    _collection = db.Professor
    for professor in _arr_obj:
        professor['assigments'] = getAssigmentsIdsByRaw(professor['assigments'])
        
        result = _collection.insert_one(professor)
        print (result.inserted_id)
        
        result = _collection.create_index([('rawName', pymongo.ASCENDING)], unique=True)
        print (sorted(list(_collection.index_information())))

def create_assigned_hour(_obj):
    _collection = db.Hour_Assigned
    result = _collection.insert_one(_obj)
    print ('assignedId: ' + str(result.inserted_id))
    print ('-- al ProfesorId: ' + str(_obj['owner']))
    cursor = db.Professor.update(
        { '_id': ObjectId(_obj['owner'])}, 
        { '$push': { 'assigned_hours': result.inserted_id } })

def in_assigned_hours_get_ids(_obj):
    _obj['assigment'] = getAssigmentIdByRaw(_obj['assigment'])
    _obj['owner'] = getProfessorIdByRaw(_obj['owner'])
    _obj['room'] = getRoomIdByRoom(_obj['room'])
    return _obj

def getProfessorIdByRaw(_rawName):
    result = db.Professor.find_one({'rawName': _rawName})
    return result['_id']

def getAssigmentIdByRaw(_rawName):
    result = db.Assigment.find_one({'rawName': _rawName})
    return result['_id']

def getRoomIdByRoom(_room):
    result = db.Room.find_one({'room': _room})
    return result['_id']

def getAssigmentsIdsByRaw(_arr_obj):
    _assigments_ids = []       
    for assigment in _arr_obj:
        result = db.Assigment.find_one({'rawName': assigment['rawName']})
        _assigments_ids.append(result['_id'])
    return _assigments_ids

def getProfessorIdsByRaw(_arr_obj):
    _assigments_ids = []       
    for assigment in _arr_obj:
        result = db.Professor.find_one({'rawName': assigment['rawName']})
        _assigments_ids.append(result['_id'])
    return _assigments_ids

#Serialize
def serialize_room(_obj):
    
    return {
        "room": _obj[SALON],
        "barcode": _obj[SALON],
        "building": "none",
        "order": []
    }

def serialize_assigment(_obj):
    return {
        "rawName": _obj[MATERIA],
        "name": re.sub(r'\([^)]*\)', '', _obj[MATERIA]),
        "plan": _obj[PLAN],
        "code": _obj[CLAVE],
        "type": _obj[LAB]
    }

def serialize_teacher(_obj):
    _teacher_splited = _obj[NOMBRE].split(' ')
    if(_teacher_splited[-1] == '' or len(_teacher_splited[-1]) < 0):
        _teacher_splited[-1] = 'ING.'
    return {
        "rawName": _obj[NOMBRE],
        "name": _teacher_splited[2].title(),
        "lastName": _teacher_splited[0].title() + ' ' + _teacher_splited[1].title(),  
        "fingerPrint": '',
        "employeeNumber": _obj[EMPLEADO],
        "userType": "Professor",
        "assigments": [] 
    }

def serialize_assigned_hours(_obj):
    _arr_academy_hours = { "count": 0, "data": [] }
    _day = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado']


    _arr_academy_hours['data'].append({

        "day": _day[int(_obj[DIA])],
        "academyHour": _obj[HORA],
        "room": _obj[SALON],
        "assigment": _obj[MATERIA],
        "owner": _obj[NOMBRE],
        "modality": _obj[NEXUS],
        "type": _obj[LAB],
        "group": _obj[GRUPO],
        "period": _obj[PERIODO]

    })
    
    _arr_academy_hours['count'] = len(_arr_academy_hours['data'])
    return _arr_academy_hours

def getUniquesInArrayDict(_dict, key):
    return { v[key]:v for v in _dict }.values()


def __init__():
    
    _professors = []
    _assigments = []
    _rooms = []
    
    _full_assigments = []

    file = open("csv.csv")
    csvFile = csv.DictReader(file)

    for assigment in csvFile:
        
        _full_assigments.append(assigment)
        _rooms.append(serialize_room(assigment))
        _assigments.append(serialize_assigment(assigment))
        _professors.append(serialize_teacher(assigment))
    
    _rooms_uniq = getUniquesInArrayDict(_rooms, 'room')
    _assigments_uniq = getUniquesInArrayDict(_assigments, 'rawName')
    _professors_uniq = getUniquesInArrayDict(_professors, 'rawName')

    for professor in _professors_uniq:
        for assigment in _full_assigments:
            if assigment[NOMBRE] == professor['rawName']:
                _obj = serialize_assigment(assigment)
                professor['assigments'].append(_obj)

        professor['assigments'] = getUniquesInArrayDict(professor['assigments'], 'rawName')
    
    print ('-- Guardamos: ')
    create_rooms_db(_rooms_uniq)
    create_assigments_db(_assigments_uniq)
    create_professors_db(_professors_uniq)
    

    _assigneds = [ serialize for raw_assigment in _full_assigments for serialize in serialize_assigned_hours(raw_assigment)['data']]
   
    for assigned_hour in _assigneds:
        assigned_hour = in_assigned_hours_get_ids(assigned_hour)
        create_assigned_hour(assigned_hour)


__init__()
