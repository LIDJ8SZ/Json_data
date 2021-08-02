import pandas as pd
import json
from pyspark.sql.types import (StructField, StringType, StructType, IntegerType)
import os
import pandasql as ps
import warnings

def get_patient_info(emr,table1,table2,table3):
    dem = emr['demographics']
    patInfo_dict = {}
    
    #Create a table out of the available demographic info
    pat_data = {'demographicID' : dem.get('id'),
                'dob' : dem.get('dob'),
                'sex' : dem.get('sex'),
                'expirationIndicator' : dem.get('expirationIndicator'),
                'patientClass' : dem.get('patientClass')
        
    }
    dem_data = pd.DataFrame(pat_data,index = [0])
    
    #Concatenate global variables
    dem_data.insert(0,'patient_id',emr['id'])
    dem_data.insert(1,'PAA_version',emr['version'])
    dem_data.insert(2, 'admitDate', emr['admitDate'])
    dem_data.insert(3, 'dischargeDate', emr['dischargeDate'])
    
    #Create name and identifier tables
    for keys in dem.keys():
        if keys == 'name':
            #name_pd = pd.DataFrame.from_dict(dem[keys], orient="index")
            pat_name = dem[keys]
            if not pat_name:
                continue
            else:
                name_dict = {'patient_id' : emr['id'] , 
                             'firstName' : pat_name.get('first'), 
                             'lastName' : pat_name.get('last'),
                             'middleInitial' : pat_name.get('middle'), 
                             'title' : pat_name.get('title') , 
                             'suffix' : pat_name.get('suffix')}
                name_df = pd.DataFrame(name_dict,index = [0])
        elif keys == 'identifiers':
            dem_id = dem[keys]
            if not dem_id:
                id_pd = None
            else:
                iterator = 0
                for i in dem_id:
                    id_dict = {
                            'patient_id' : emr['id'],
                            'PAA_version' : emr['version'],
                            'additionalDemID' : i.get('id'),
                            'additionalDemAuthority' : i.get('authority'),
                            'additionalDemType' : i.get('type'),
                            'additionalDemFacility' : i.get('facility')
                              }
                    if iterator == 0:
                        id_pd = pd.DataFrame(id_dict,index = [0])
                        iterator += 1
                    else:
                        id_dict_union = {
                            'patient_id' : emr['id'],
                            'PAA_version' : emr['version'],
                            'additionalDemID' : i.get('id'),
                            'additionalDemAuthority' : i.get('authority'),
                            'additionalDemType' : i.get('type'),
                            'additionalDemFacility' : i.get('facility')
                              }
                        id_pd_union = pd.DataFrame(id_dict_union,index=[0])
                        frames_id = [id_pd,id_pd_union]
                        id_pd = pd.concat(frames_id,axis = 0,join = "outer",ignore_index=True)
     
    #Create a separate table for contacts information
    if dem.get('phoneNumbers') is not None or dem.get('emergencyContacts') is not None:
        if dem['phoneNumbers'] or dem['emergencyContacts']:
            pat_contacts = pd.DataFrame({'phoneNumer' : dem.get('phoneNumbers'),'emergencyContacts' : dem.get('emergencyContacts')})
            pat_contacts.insert(0,'patient_id',emr['id'])
            pat_contacts.insert(1,'PAA_version',emr['version'])
        else :
            pat_contacts = None
    else:
        pat_contacts = None
     
    #Merge name and demographic tables to get the patient_info 
    pat_info = pd.merge(left=dem_data, right=name_df, how='inner', left_on='patient_id', right_on='patient_id')
    pat_info_cols = ['patient_id','PAA_version','admitDate', 'dischargeDate','demographicID','firstName','lastName','middleInitial','title','suffix','dob','sex','expirationIndicator','patientClass']
    identifier_cols = ['patient_id','PAA_version','additionalDemID','additionalDemAuthority','additionalDemType','additionalDemFacility']
    patient_Info = pat_info[pat_info_cols]
    identifiers = id_pd[identifier_cols]
    
    #Store the final output dataframes as a dictionary 
    patInfo_dict[table1] = patient_Info
    patInfo_dict[table2] = identifiers
    patInfo_dict[table3] = pat_contacts
    return patInfo_dict
  
 def get_discharge_info(emr,table1,table2):
    disc = emr['dischargeInfo']
    discInfo_dict = {}
    
    ####################################### Columnar Display ###########################################################
    destination = disc['destinationDescription'] if 'destinationDescription' in disc.keys() else disc['destination']  #Incorporating additional logic to include members from April 23 to April 30
    discharge_dict = {
            'patient_id' : emr['id'],
            'PAA_version' : emr['version'],
            'inception' : disc.get('inception'),
            'disposition' : disc.get('disposition') ,
            'destination_id' : destination.get('id') if isinstance(destination, dict) else destination,
            'destination_name' : destination.get('name') if isinstance(destination, dict) else destination,
            'destination_careSetting' : destination.get('careSetting') if isinstance(destination, dict) else destination
        }
    disc_info = pd.DataFrame(discharge_dict,index = [0])
    if isinstance(destination,dict) and destination['alternateIdentifiers']:
            iterator = 0
            for i in destination['alternateIdentifiers']:
                alt_id_dict = {
            'patient_id' : emr['id'],
            'PAA_version' : emr['version'],
            'destination_id' :  i.get('id'),
            'destination_authority' : i.get('authority'),
            'destination_facility' : i.get('facility'),
            'destination_type' : i.get('type')
                              }
                if iterator == 0:
                        dest_alt_id_pd = pd.DataFrame(alt_id_dict,index = [0])
                        iterator += 1
                else:
                    alt_id_dict_union = {
            'patient_id' : emr['id'],
            'PAA_version' : emr['version'],
            'destination_id' :  i.get('id'),
            'destination_authority' : i.get('authority'),
            'destination_facility' : i.get('facility'),
            'destination_type' : i.get('type')
                              }
                    dest_alt_id_pd_union = pd.DataFrame(alt_id_dict_union,index=[0])
                    frames_id = [dest_alt_id_pd,dest_alt_id_pd_union]
                    dest_alt_id_pd = pd.concat(frames_id,axis = 0,join = "outer",ignore_index=True)
    else:
        dest_alt_id_pd = None

        
    ##################################### Row Level Display (Confirm with Sean)######################################################
    #dataframe = pd.DataFrame.from_dict(disc['dischargeInfo'], orient="index")
    #disc_info = dataframe.transpose()
    #disc_info.insert(0,'patient_id',emr['id'])
    #disc_info.insert(1,'version',emr['version'])
    discInfo_dict[table1] = disc_info
    discInfo_dict[table2] = dest_alt_id_pd
    return discInfo_dict
  
def get_diagnoses_info(emr,table):
    diag = {}
    diagnoses = emr['diagnoses']
    if diagnoses:
                iterator = 0
               
                for i in diagnoses:
                    diag_dict = {
                                    'patient_id' : emr['id'],
                                    'PAA_version' : emr['version'],
                                    'method' :  i.get('method'),
                                    'code' : i.get('code'),
                                    'description' : i.get('description'),
                                    'priority' : i.get('priority'),
                                    'type' : i.get('type')
                                }
                    if iterator == 0:
                            diagnoses_data = pd.DataFrame(diag_dict,index = [0])
                            iterator += 1
                    else:
                        diagnoses_data_union = {
                                                    'patient_id' : emr['id'],
                                                    'PAA_version' : emr['version'],
                                                    'method' :  i.get('method'),
                                                    'code' : i.get('code'),
                                                    'description' : i.get('description'),
                                                    'priority' : i.get('priority'),
                                                    'type' : i.get('type')
                                               }
                        diagnoses_data_union = pd.DataFrame(diagnoses_data_union,index=[0])
                        frames_diag = [diagnoses_data,diagnoses_data_union]
                        diagnoses_data = pd.concat(frames_diag,axis = 0,join = "outer",ignore_index=True)
    else:
        diagnoses_data = None
        
    diag[table] = diagnoses_data
    return diag
  
def get_payors_info(emr,table):
    payors = {}
    mem_payors = emr['payors']
    if mem_payors:
                iterator = 0
               
                for i in mem_payors:
                    if 'policy' in i.keys(): 
                        payors_dict = {
                                    'patient_id' : emr['id'],
                                    'PAA_version' : emr['version'],
                                    'companyName' :  i['policy']['payor']['name']['name'], #Implementing additional logic to incorporate pre build members
                                    'policyNumber' : i['policy']['policyNumber'],
                                    'effectiveDate' : i.get('effectiveDate'),
                                    'expiresDate' : i.get('expiresDate'),
                                    'priority' : i.get('priority')
                                      }
                    else:
                        payors_dict = {
                                    'patient_id' : emr['id'],
                                    'PAA_version' : emr['version'],
                                    'companyName' :  i.get('companyName'),
                                    'policyNumber' : i.get('policyNumber'),
                                    'effectiveDate' : i.get('effectiveDate'),
                                    'expiresDate' : i.get('expiresDate'),
                                    'priority' : i.get('priority')
                                }
                    if iterator == 0:
                            payors_data = pd.DataFrame(payors_dict,index = [0])
                            iterator += 1
                    else:
                        if 'policy' in i.keys(): 
                            payors_dict_union = {
                                        'patient_id' : emr['id'],
                                        'PAA_version' : emr['version'],
                                        'companyName' :  i['policy']['payor']['name']['name'],
                                        'policyNumber' : i['policy']['policyNumber'],
                                        'effectiveDate' : i.get('effectiveDate'),
                                        'expiresDate' : i.get('expiresDate'),
                                        'priority' : i.get('priority')
                                          }
                        else:
                            payors_dict_union = {
                                                        'patient_id' : emr['id'],
                                                        'PAA_version' : emr['version'],
                                                        'companyName' :  i.get('companyName'),
                                                        'policyNumber' : i.get('policyNumber'),
                                                        'effectiveDate' : i.get('effectiveDate'),
                                                        'expiresDate' : i.get('expiresDate'),
                                                        'priority' : i.get('priority')
                                                   }
                        payors_data_union = pd.DataFrame(payors_dict_union,index=[0])
                        frames_payors = [payors_data,payors_data_union]
                        payors_data = pd.concat(frames_payors,axis = 0,join = "outer",ignore_index=True)
    else:
        payors_data = None
        
    payors[table] = payors_data
    return payors
  
def get_facility_info(emr,table1,table2):
    facilityInfo_dict = {}
    facilitymet = emr['facility'] 
    if facilitymet is not None:
        facility_dict = {
            'patient_id' : emr['id'],
            'PAA_version' : emr['version'],
            'facilityId' : facilitymet.get('id'),
            'facilityName' : facilitymet.get('name') ,
            'facilityCareSetting' : facilitymet.get('careSetting')
        }
        facility_info = pd.DataFrame(facility_dict,index = [0])
        if isinstance(facilitymet,dict) and facilitymet['alternateIdentifiers']:
                iterator = 0
                for i in facilitymet['alternateIdentifiers']:
                    alt_id_dict = {
                'patient_id' : emr['id'],
                'PAA_version' : emr['version'],
                'facilityAltid' :  i.get('id'),
                'facilityAltauthority' : i.get('authority'),
                'facilityAltfacility' : i.get('facility'),
                'facilityAlttype' : i.get('type')
                                  }
                    if iterator == 0:
                            facility_alt_id_pd = pd.DataFrame(alt_id_dict,index = [0])
                            iterator += 1
                    else:
                        alt_id_dict_union = {
                'patient_id' : emr['id'],
                'PAA_version' : emr['version'],
                'facilityAltid' :  i.get('id'),
                'facilityAltauthority' : i.get('authority'),
                'facilityAltfacility' : i.get('facility'),
                'facilityAlttype' : i.get('type')
                                  }
                        facility_alt_id_pd_union = pd.DataFrame(alt_id_dict_union,index=[0])
                        frames_id = [facility_alt_id_pd,facility_alt_id_pd_union]
                        facility_alt_id_pd = pd.concat(frames_id,axis = 0,join = "outer",ignore_index=True)
        else:
            facility_alt_id_pd = None
    else:
        facility_info = None
        
    facilityInfo_dict[table1] = facility_info
    facilityInfo_dict[table2] = facility_alt_id_pd
    return facilityInfo_dict
  
  
def get_documents_info(emr,table):
    docs = {}
    documents_list = emr['documents']
    if documents_list:
                iterator = 0
                for i in documents_list:
                    docs_dict = {
                                    'patient_id' : emr['id'],
                                    'PAA_version' : emr['version'],
                                    'fileName' :  i.get('filename'),
                                    'displayFilename' : i.get('displayFilename'),
                                    'filetype' : i.get('filetype'),
                                    'dataType' : i.get('dataType')
                                }
                    if iterator == 0:
                            docs_data = pd.DataFrame(docs_dict,index = [0])
                            iterator += 1
                    else:
                        docs_dict_union = {
                                                     'patient_id' : emr['id'],
                                                     'PAA_version' : emr['version'],
                                                     'fileName' :  i.get('filename'),
                                                     'displayFilename' : i.get('displayFilename'),
                                                     'filetype' : i.get('filetype'),
                                                     'dataType' : i.get('dataType')
                                               }
                        documents_data_union = pd.DataFrame(docs_dict_union,index=[0])
                        frames_docs = [docs_data,documents_data_union]
                        docs_data = pd.concat(frames_docs,axis = 0,join = "outer",ignore_index=True)
    else:
        docs_data = None
        
    docs[table] = docs_data
    return docs
  
  def get_oasis_info(emr,table,csv):
    oasis = {}
    oasis_inf = None
    if 'assessments' in emr.keys():
        if emr['assessments']:
            assessments = emr['assessments']
            iterator = 0
            for line in assessments:
                oasis_dict = {}
                oasis_data = line['assessmentData']
                oasis_cols = csv['Colnames']
                if line['type'] == 'oasis':
                    oasis_dict = {
                        'patient_id' : emr['id'],
                        'PAA_version' : emr['version'],
                        'oasis_id' : line.get('id'),
                        'oasis_occurredAt' : line.get('occurredAt')
                    }
                    for itm in oasis_cols:
                        oasis_dict.update({itm:oasis_data.get(itm)})
                    if iterator == 0:
                        oasis_inf = pd.DataFrame(oasis_dict,index=[0])
                        iterator +=1
                    else:
                        oasis_dict_union = {
                        'patient_id' : emr['id'],
                        'PAA_version' : emr['version'],
                        'oasis_id' : line.get('id'),
                        'oasis_occurredAt' : line.get('occurredAt')
                                    }
                        for itm in oasis_cols:
                            oasis_dict_union.update({itm:oasis_data.get(itm)})
                        oasis_inf_union = pd.DataFrame(oasis_dict_union,index=[0])
                        frames_oasis = [oasis_inf,oasis_inf_union]
                        oasis_inf = pd.concat(frames_oasis,axis = 0,join = "outer",ignore_index=True)
                else:
                    continue
        else:
            oasis_inf = None
    else:
        oasis_inf = None
    oasis[table] = oasis_inf if oasis_inf is not None else None
    return oasis
  
def get_mds_info(emr,table,csv):
    mds = {}
    mds_inf = None
    if 'assessments' in emr.keys():
        if emr['assessments']:
            assessments = emr['assessments']
            iterator = 0
            for line in assessments:
                mds_dict = {}
                mds_data = line['assessmentData']
                mds_cols = csv['Colnames']
                if line['type'] == 'mds':
                    mds_dict = {
                        'patient_id' : emr['id'],
                        'PAA_version' : emr['version'],
                        'mds_id' : line.get('id'),
                        'mds_occurredAt' : line.get('occurredAt')
                    }
                    for itm in mds_cols:
                        mds_dict.update({itm:mds_data.get(itm)})
                    if iterator == 0:
                        mds_inf = pd.DataFrame(mds_dict,index=[0])
                        iterator += 1
                    else:
                        mds_dict_union = {
                        'patient_id' : emr['id'],
                        'PAA_version' : emr['version'],
                        'mds_id' : line.get('id'),
                        'mds_occurredAt' : line.get('occurredAt')
                                        }
                        for itm in mds_cols:
                            mds_dict_union.update({itm:mds_data.get(itm)})
                        mds_inf_union = pd.DataFrame(mds_dict_union,index=[0])
                        frames_mds = [mds_inf,mds_inf_union]
                        mds_inf = pd.concat(frames_mds,axis = 0,join = "outer",ignore_index=True)
                else:
                    continue
        else:
            mds_inf = None
    else:
        mds_inf = None
    mds[table] = mds_inf if mds_inf is not None else None
    return mds
  
  
def compile_pat_info(json_py,tables_json):
    pat_info_iterator = 0
    emr = {}
    patientInfo = tables_json['pat_info'][0]
    demIdentifiers = tables_json['pat_info'][1]
    contacts_table = tables_json['pat_info'][2] 
    patInfo = None
    for line in json_py:
        patient_info_data = get_patient_info(line,patientInfo,demIdentifiers,contacts_table)
        if pat_info_iterator == 0:
            patInfo = patient_info_data[patientInfo]
            identifiers = patient_info_data[demIdentifiers]
            contacts = patient_info_data[contacts_table] 
            pat_info_iterator += 1
        else:
            frames_pat = [patInfo,patient_info_data[patientInfo]]
            patInfo = pd.concat(frames_pat,axis = 0,join = "outer",ignore_index=True)
            frames_id = [identifiers,patient_info_data[demIdentifiers]]
            identifiers = None if frames_id[0] is None and frames_id[1] is None else pd.concat(frames_id,axis = 0,join = "outer",ignore_index=True)
            frames_contact = [contacts,patient_info_data[contacts_table]]
            contacts = None if frames_contact[0] is None and frames_contact[1] is None else pd.concat(frames_contact,axis = 0,join = "outer",ignore_index=True)
    emr[patientInfo] = patInfo if patInfo is not None else None
    emr[demIdentifiers] = identifiers 
    emr[contacts_table] = contacts
    return emr
  
 def compile_discharge_info(json_py,tables_json):
    discharge_iterator = 0
    emr = {}
    dischargeInfo = tables_json['dischargeInfo'][0]
    dischargedestId = tables_json['dischargeInfo'][1]
    discharge_info = None
    for line in json_py:
        discharge_data = get_discharge_info(line,dischargeInfo,dischargedestId) if 'dischargeInfo' in line.keys() and line['dischargeInfo'] is not None else None
        if discharge_data is not None:
            if discharge_iterator == 0:
                discharge_info = discharge_data[dischargeInfo]
                discharge_destination_id = discharge_data[dischargedestId]
                discharge_iterator += 1
            else:
                frames_discharge = [discharge_info,discharge_data[dischargeInfo]]
                discharge_info = pd.concat(frames_discharge,axis = 0,join = "outer",ignore_index=True)
                frames_discharge_dest_id = [discharge_destination_id,discharge_data[dischargedestId]]
                discharge_destination_id =  None if frames_discharge_dest_id[0] is None and frames_discharge_dest_id[1] is None else pd.concat(frames_discharge_dest_id,axis = 0,join = "outer",ignore_index=True)
    emr[dischargeInfo] = discharge_info if discharge_info is not None else None
    emr[dischargedestId] = discharge_destination_id
    return emr
  
def compile_diagnoses_info(json_py,tables_json):
    diagnoses_iterator = 0
    emr = {}
    diagnosesInfo = tables_json['diagnosesInfo']
    diagnoses_info = None
    for line in json_py:
        diagnoses_data = get_diagnoses_info(line,diagnosesInfo) if 'diagnoses' in line.keys() and line['diagnoses'] is not None else None
        if diagnoses_data is not None:
            if diagnoses_iterator == 0:
                diagnoses_info = diagnoses_data[diagnosesInfo]
                diagnoses_iterator += 1
            else:
                frames_diagnoses = [diagnoses_info,diagnoses_data[diagnosesInfo]] 
                diagnoses_info = None if frames_diagnoses[0] is None and frames_diagnoses[1] is None else pd.concat(frames_diagnoses,axis = 0,join = "outer",ignore_index=True)
    emr[diagnosesInfo] = diagnoses_info if diagnoses_info is not None else None
    return emr
  
def compile_payor_info(json_py,tables_json):
    payor_iterator = 0
    emr = {}
    payorInfo = tables_json['payorInfo']
    payors_info = None
    for line in json_py:
        payors_data = get_payors_info(line,payorInfo) if 'payors' in line.keys() and line['payors'] is not None else None
        if payors_data is not None:
            if payor_iterator == 0:
                payors_info = payors_data[payorInfo]
                payor_iterator += 1
            else:
                frames_payors = [payors_info,payors_data[payorInfo]] 
                payors_info = None if frames_payors[0] is None and frames_payors[1] is None else pd.concat(frames_payors,axis = 0,join = "outer",ignore_index=True)
    emr[payorInfo] = payors_info if payors_info is not None else None
    return emr
  
def compile_facility_info(json_py,tables_json):
    facility_iterator = 0
    emr = {}
    facilityInfo = tables_json['facilityInfo'][0]
    facilityId = tables_json['facilityInfo'][1]
    facility_info = None
    for line in json_py:
        facility_data = get_facility_info(line,facilityInfo,facilityId) if 'facility' in line.keys() and line['facility'] is not None else None
        if facility_data is not None:
            if facility_iterator == 0:
                facility_info = facility_data[facilityInfo]
                facility_id = facility_data[facilityId]
                facility_iterator += 1
            else:
                frames_facility = [facility_info,facility_data[facilityInfo]] 
                facility_info = pd.concat(frames_facility,axis = 0,join = "outer",ignore_index=True)
                frames_facility_id = [facility_id,facility_data[facilityId]]
                facility_id =  None if frames_facility_id[0] is None and frames_facility_id[1] is None else pd.concat(frames_facility_id,axis = 0,join = "outer",ignore_index=True)
    emr[facilityInfo] = facility_info if facility_info is not None else None 
    emr[facilityId] = facility_id
    return emr

def compile_documents_info(json_py,tables_json):
    documents_iterator = 0
    emr = {}
    documentInfo = tables_json['documentsInfo']
    documents_info = None
    for line in json_py:
        documents_data = get_documents_info(line,documentInfo) if 'documents' in line.keys() and line['documents'] is not None else None
        if documents_data is not None:
            if documents_iterator == 0:
                documents_info = documents_data[documentInfo]
                documents_iterator += 1
            else:
                frames_documents = [documents_info,documents_data[documentInfo]] 
                documents_info = None if frames_documents[0] is None and frames_documents[1] is None else pd.concat(frames_documents,axis = 0,join = "outer",ignore_index=True)
    emr[documentInfo] = documents_info if documents_info is not None else None
    return emr
  
def compile_oasis_info(json_py,tables_json,csv):
    oasis_iterator = 0
    emr = {}
    oasisInfo = tables_json['oasisInfo']
    oasis_info = None
    for line in json_py:
        oasis_data = get_oasis_info(line,oasisInfo,csv) if 'assessments' in line.keys() and line['assessments'] is not None else None
        if oasis_data is not None:
            if oasis_iterator == 0:
                oasis_info = oasis_data[oasisInfo]
                oasis_iterator += 1
            else:
                frames_oasis = [oasis_info,oasis_data[oasisInfo]] 
                oasis_info = None if frames_oasis[0] is None and frames_oasis[1] is None else pd.concat(frames_oasis,axis = 0,join = "outer",ignore_index=True)
    emr[oasisInfo] = oasis_info if oasis_info is not None else None
    return emr
  
  
def compile_mds_info(json_py,tables_json,csv):
    mds_iterator = 0
    emr = {}
    mdsInfo = tables_json['mdsInfo']
    mds_info = None
    for line in json_py:
        mds_data = get_mds_info(line,mdsInfo,csv) if 'assessments' in line.keys() and line['assessments'] is not None else None
        if mds_data is not None:
            if mds_iterator == 0:
                mds_info = mds_data[mdsInfo]
                mds_iterator += 1
            else:
                frames_mds = [mds_info,mds_data[mdsInfo]] 
                mds_info = None if frames_mds[0] is None and frames_mds[1] is None else pd.concat(frames_mds,axis = 0,join = "outer",ignore_index=True)
    emr[mdsInfo] = mds_info if mds_info is not None else None
    return emr
  
  
def remove_complete_duplicates(df):
    emr = {}
    for keys in df.keys():
        if df[keys] is not None:
            emr[keys] = df[keys].drop_duplicates(subset=[cols for cols in df[keys]],
                                                    keep='first').reset_index(drop=True)
        else:
            emr[keys] = None
    return emr
  
  
def dq_check_patInfo(json_py,emr):
    if len(json_py) == len(emr['patientInfo']):
        print("Row Count for patient info table Matched : {}".format(len(json_py)))
    else:
        warnings.warn("Row Count mismatch for patient info table \n EMR Patient Info Count : {count1} \n Table Count : {count2}".format(count1 = len(emr['patientInfo']),count2 = len(json_py)),DeprecationWarning,stacklevel=2)
    df_pat = emr['patientInfo']
    if len(emr['patientInfo']) > 0:
        pat_id = sum(pd.isnull(df_pat['patient_id']))
        version = sum(pd.isnull(df_pat['PAA_version'])) 
        admit_date = sum(pd.isnull(df_pat['admitDate']))
        first_name = sum(pd.isnull(df_pat['firstName']))
        if pat_id > 0 or version > 0 or admit_date > 0 or first_name > 0:
            warnings.warn("There are nulls in one or more primary keys in the patient info table: \n Patient_ID null Count : {nullc1} \n version null count : {nullc2} \n admitDate null count : {nullc3} \n firstName null count : {nullc4}".format(nullc1 = pat_id, nullc2 = version, nullc3 = admit_date, nullc4 = first_name))
        else:
            print("Completeness check for patient info table passed")
    else:
        print("Completenes Check Not Applicable")
        
        
def dq_checks_demographicId(json_py,emr):
    iterator_demid = 0
    for line in json_py:
        dem = line['demographics']
        if dem.get('identifiers') is not None:
            py = len(dem['identifiers'])
            if py >= 1:
                iterator_demid += py
            else:
                continue
        else:
            continue
    table_count = len(emr['demographicIdentifiers']) if emr['demographicIdentifiers'] is not None else 0
    if iterator_demid == table_count:
        print("Row Count for demographicIdentifiers table Matched : {}".format(iterator_demid))
    else:
        warnings.warn("Row Count mismatch for Contacts table \n EMR demographicIdentifiers Count : {count1} \n Table Count : {count2}".format(count1 = iterator_demid,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_demId = emr['demographicIdentifiers']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_demId['patient_id']))
        version = sum(pd.isnull(df_demId['PAA_version']))
        demId = sum(pd.isnull(df_demId['additionalDemID']))
        if pat_id > 0 or version > 0 or demId > 0:
            warnings.warn("There are nulls present in one or more primary keys in the demographic identifiers table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n Additional Demographics ID null count : {nullc3}".format(nullc1 = pat_id,nullc2 = version,nullc3 = demId),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for Contacts Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def dq_check_contacts(json_py,emr):
    iterator_cont = 0
    for line in json_py:
        dem = line['demographics']
        py1 = len(dem['phoneNumbers']) if dem.get('phoneNumbers') is not None else 0
        py2 = len(dem['emergencyContacts']) if dem.get('emergencyContacts') is not None else 0
        py = py1+py2
        if py >= 1:
                iterator_cont += py
        else:
                continue
    table_count = len(emr['Contacts']) if emr['Contacts'] is not None else 0
    if iterator_cont == table_count:
        print("Row Count for Contacts table Matched : {}".format(iterator_cont))
    else:
        warnings.warn("Row Count mismatch for Contacts table \n EMR Contacts Count : {count1} \n Table Count : {count2}".format(count1 = iterator_cont,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_contacts = emr['Contacts']
    if iterator_cont > 0:
        pat_id = sum(pd.isnull(df_contacts['patient_id']))
        version = sum(pd.isnull(df_contacts['PAA_version']))
        if pat_id > 0 or version > 0:
            warnings.warn("NULLS present in Contacts table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2}".format(nullc1 = pat_id,nullc2 = version),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for Contacts Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
def dq_check_discharge(json_py,emr):
    iterator_disc = 0
    for line in json_py:
        disc = line.get('dischargeInfo')
        if disc is not None:
            iterator_disc += 1
        else:
            continue
    table_count = len(emr['discharge']) if emr['discharge'] is not None else 0
    if iterator_disc == table_count:
        print("Row Count for discharge table Matched : {}".format(iterator_disc))
    else:
        warnings.warn("Row Count mismatch for discharge table \n EMR discharge Count : {count1} \n Table Count : {count2}".format(count1 = iterator_disc,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_disc = emr['discharge']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_disc['patient_id']))
        version = sum(pd.isnull(df_disc['PAA_version']))
        disposition = sum(pd.isnull(df_disc['disposition']))
        if pat_id > 0 or version > 0 or (disposition and destination_description) > 0:
            warnings.warn("NULLS present in discharge table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n disposition null count : {nullc3}".format(nullc1 = pat_id,nullc2 = version,nullc3 = disposition),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for discharge Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def dq_check_dischargeIdentifiers(json_py,emr):
    iterator_discId = 0
    for line in json_py:
        disc = line.get('dischargeInfo')
        if disc is not None:
            disc_Id = disc['destinationDescription'] if 'destinationDescription' in disc.keys() else disc['destination']
        else:
            disc_Id = {}
        if disc_Id.get('alternateIdentifiers') is not None:
            py = len(disc_Id['alternateIdentifiers'])
            if py >= 1:
                iterator_discId += py
            else:
                continue
        else:
            continue
    table_count = len(emr['dischargeDestinationIdentifiers']) if emr['dischargeDestinationIdentifiers'] is not None else 0
    if iterator_discId == table_count:
        print("Row Count for discharge destination ID table Matched : {}".format(iterator_discId))
    else:
        warnings.warn("Row Count mismatch for discharge destination ID table \n EMR discharge destination ID Count : {count1} \n Table Count : {count2}".format(count1 = iterator_discId,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_discId = emr['dischargeDestinationIdentifiers']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_discId['patient_id']))
        version = sum(pd.isnull(df_discId['PAA_version']))
        destination_id = sum(pd.df_discId(df_disc['destination_id']))
        if pat_id > 0 or version > 0 or destination_id > 0:
            warnings.warn("NULLS present in discharge destination ID table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n destination Id null count : {nullc3}".format(nullc1 = pat_id,nullc2 = version,nullc3 = destination_id),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for discharge destination ID Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
def dq_checks_diagnoses(json_py,emr):
    iterator_diag = 0
    for line in json_py:
        diag = line.get('diagnoses')
        py = len(diag) if diag is not None else 0
        if py >= 1:
            iterator_diag += py
        else:
            continue
    table_count = len(emr['diagnoses']) if emr['diagnoses'] is not None else 0
    if iterator_diag == table_count:
        print("Row Count for diagnoses table Matched : {}".format(iterator_diag))
    else:
        warnings.warn("Row Count mismatch for diagnoses table \n EMR diagnoses Count : {count1} \n Table Count : {count2}".format(count1 = iterator_diag,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_diag = emr['diagnoses']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_diag['patient_id']))
        version = sum(pd.isnull(df_diag['PAA_version']))
        code = sum(pd.isnull(df_diag['code']))
        method = sum(pd.isnull(df_diag['method']))
        type_diag = sum(pd.isnull(df_diag['type']))
        if pat_id > 0 or version > 0 or code > 0 or method > 0:
            warnings.warn("NULLS present in diagnoses table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n code null count : {nullc3} \n method null count : {nullc4} \n type null count : {nullc5}".format(nullc1 = pat_id,nullc2 = version,nullc3 = demId, nullc4 = method,nullc5=type_diag),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for diagnoses Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
def dq_checks_payors(json_py,emr):
    iterator_payors = 0
    for line in json_py:
        payors = line['payors']
        py = len(payors)
        if py >= 1:
            iterator_payors += py
        else:
            continue
    table_count = len(emr['payors']) if emr['payors'] is not None else 0
    if iterator_payors == table_count:
        print("Row Count for payors table Matched : {}".format(iterator_payors))
    else:
        warnings.warn("Row Count mismatch for payors table \n EMR payors Count : {count1} \n Table Count : {count2}".format(count1 = iterator_payors,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_payors = emr['payors']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_payors['patient_id']))
        version = sum(pd.isnull(df_payors['PAA_version']))
        companyName = sum(pd.isnull(df_payors['companyName']))
        policyNumber = sum(pd.isnull(df_payors['policyNumber']))
        if pat_id > 0 or version > 0 or companyName > 0 or policyNumber > 0:
            warnings.warn("NULLS present in payors table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n companyName null count : {nullc3} \n policyNumber null count : {nullc4}".format(nullc1 = pat_id,nullc2 = version,nullc3 = companyName, nullc4 = policyNumber),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for payors Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
def dq_checks_facility(json_py,emr):
    iterator_fac = 0
    for line in json_py:
        facility = line.get('facility')
        if facility is not None:
            iterator_fac += 1
        else:
            continue
    table_count = len(emr['facility']) if emr['facility'] is not None else 0
    if iterator_fac == table_count:
        print("Row Count for facility table Matched : {}".format(iterator_fac))
    else:
        warnings.warn("Row Count mismatch for facility table \n EMR facility Count : {count1} \n Table Count : {count2}".format(count1 = iterator_fac,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_facility = emr['facility']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_facility['patient_id']))
        version = sum(pd.isnull(df_facility['PAA_version']))
        facilityId = sum(pd.isnull(df_facility['facilityId']))
        facilityName = sum(pd.isnull(df_facility['facilityName']))
        careSetting = sum(pd.isnull(df_facility['facilityCareSetting']))
        if pat_id > 0 or version > 0 or facilityId > 0 or facilityName > 0:
            warnings.warn("NULLS present in facility table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n facilityId null count : {nullc3} \n facilityName null count : {nullc4} \n careSetting null count : {nullc5}".format(nullc1 = pat_id,nullc2 = version,nullc3 = facilityId, nullc4 = facilityName, nullc5=careSetting),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for facility Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def dq_checks_facilityId(json_py,emr):
    iterator_facid = 0
    for line in json_py:
        facility = line.get('facility')
        if facility is not None:
            if facility.get('alternateIdentifiers') is not None:
                py = len(facility['alternateIdentifiers'])
                if py >= 1:
                    iterator_facid += py
                else:
                    continue
            else:
                continue
        else:
            continue
    table_count = len(emr['facilityIdentifiers']) if emr['facilityIdentifiers'] is not None else 0
    if iterator_facid == table_count:
        print("Row Count for facility identifiers table Matched : {}".format(iterator_facid))
    else:
        warnings.warn("Row Count mismatch for facility identifiers table \n EMR facility identifiers Count : {count1} \n Table Count : {count2}".format(count1 = iterator_facid,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_facilityId = emr['facilityIdentifiers']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_facilityId['patient_id']))
        version = sum(pd.isnull(df_facilityId['PAA_version']))
        facilityAltid = sum(pd.isnull(df_facilityId['facilityAltid']))
        facilityAlttype = sum(pd.isnull(df_facilityId['facilityAlttype']))
        if pat_id > 0 or version > 0 or facilityAltid > 0 or facilityAlttype > 0:
            warnings.warn("NULLS present in facility identifiers table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n facilityAltid null count : {nullc3} \n facilityAlttype null count : {nullc4}".format(nullc1 = pat_id,nullc2 = version,nullc3 = facilityAltid, nullc4 = facilityAlttype),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for facility identifiers Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def dq_checks_documents(json_py,emr):
    iterator_docs = 0
    for line in json_py:
        docs = line.get('documents')
        py = len(docs) if docs is not None else 0
        if py >= 1:
            iterator_docs += py
        else:
            continue
    table_count = len(emr['documents']) if emr['documents'] is not None else 0
    if iterator_docs == table_count:
        print("Row Count for documents table Matched : {}".format(iterator_docs))
    else:
        warnings.warn("Row Count mismatch for documents table \n EMR documents Count : {count1} \n Table Count : {count2}".format(count1 = iterator_docs,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_docs = emr['documents']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_docs['patient_id']))
        version = sum(pd.isnull(df_docs['PAA_version']))
        fileName = sum(pd.isnull(df_docs['fileName']))
        displayFilename = sum(pd.isnull(df_docs['displayFilename']))
        if pat_id > 0 or version > 0 or fileName > 0 or displayFilename > 0:
            warnings.warn("NULLS present in documents table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n fileName null count : {nullc3} \n displayFilename null count : {nullc4}".format(nullc1 = pat_id,nullc2 = version,nullc3 = fileName, nullc4 = displayFilename),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for documents Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def dq_checks_oasis(json_py,emr):
    iterator_oasis = 0
    for line in json_py:
        oasis = line.get('assessments')
        if oasis is not None:
            for i in oasis:
                if i['type'] == 'oasis':
                    iterator_oasis += 1
                else:
                    continue
        else:
            continue
    table_count = len(emr['oasis']) if emr['oasis'] is not None else 0
    if iterator_oasis == table_count:
        print("Row Count for oasis table Matched : {}".format(iterator_oasis))
    else:
        warnings.warn("Row Count mismatch for oasis table \n EMR oasis Count : {count1} \n Table Count : {count2}".format(count1 = iterator_oasis,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_oasis = emr['oasis']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_oasis['patient_id']))
        version = sum(pd.isnull(df_oasis['PAA_version']))
        oasis_id = sum(pd.isnull(df_oasis['oasis_id']))
        if pat_id > 0 or version > 0 or oasis_id > 0:
            warnings.warn("NULLS present in oasis table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n oasis id null count : {nullc3}".format(nullc1 = pat_id,nullc2 = version,nullc3 = oasis_id),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for oasis Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def dq_checks_mds(json_py,emr):
    iterator_mds = 0
    for line in json_py:
        mds = line.get('assessments')
        if mds is not None:
            for i in mds:
                if i['type'] == 'mds':
                    iterator_mds += 1
                else:
                    continue
        else:
            continue
    table_count = len(emr['mds']) if emr['mds'] is not None else 0
    if iterator_mds == table_count:
        print("Row Count for mds table Matched : {}".format(iterator_mds))
    else:
        warnings.warn("Row Count mismatch for mds table \n EMR mds Count : {count1} \n Table Count : {count2}".format(count1 = iterator_mds,count2 = table_count),DeprecationWarning,stacklevel=2)
    df_mds = emr['mds']
    if table_count > 0:
        pat_id = sum(pd.isnull(df_mds['patient_id']))
        version = sum(pd.isnull(df_mds['PAA_version']))
        mds_id = sum(pd.isnull(df_mds['mds_id']))
        if pat_id > 0 or version > 0 or mds_id > 0:
            warnings.warn("NULLS present in mds table \n Patient_ID null Count : {nullc1} \n version null Count : {nullc2} \n mds id null count : {nullc3}".format(nullc1 = pat_id,nullc2 = version,nullc3 = mds_id),DeprecationWarning,stacklevel=2)
        else:
            print("Completeness Check for mds Table Passed")
    else:
        print("Completeness Check Not Applicable")
        
        
        
def main():
    
    ###############################Initialize table creation####################################
    tables_json = {
                      'pat_info' : ['patientInfo',
                                    'demographicIdentifiers',
                                    'Contacts'],
                      'dischargeInfo' : ['discharge',
                                         'dischargeDestinationIdentifiers'],
                      'diagnosesInfo' : 'diagnoses',
                      'payorInfo' : 'payors',
                      'facilityInfo' : ['facility',
                                        'facilityIdentifiers'],
                      'documentsInfo' : 'documents',
                      'oasisInfo' : 'oasis',
                      'mdsInfo' : 'mds'
                   }
    ############################Read in JSON files as list from Github###############################3
    
    if not os.path.isfile("PAA.json"):
        raise FileNotFoundError
    else:
        json_file = open('PAA.json')
        json_py = []
        for line in json_file:
            json_data = json.loads(line)
            json_py.append(json_data)
    
    #######################################Read Necessary Files (OASIS and MDS Column Set)##############################################
    oasis_csv = pd.read_csv('itm_mstr(V2.30.1)FINAL09-10-2018.csv') #OASIS Column Set
    mds_csv = pd.read_csv('itm_mstr(V3.00.1)FINAL04-22-2019.csv') #MDS Column Set
    ###################################################################################################################################
            
    ###################################Convert JSON to tables#####################################################
    patient_info_data = compile_pat_info(json_py,tables_json)
    discharge_data = compile_discharge_info(json_py,tables_json) 
    diagnoses_data = compile_diagnoses_info(json_py,tables_json)
    payors_data =  compile_payor_info(json_py,tables_json)
    facility_data = compile_facility_info(json_py,tables_json)
    documents_data = compile_documents_info(json_py,tables_json)
    oasis_data = compile_oasis_info(json_py,tables_json,oasis_csv)
    mds_data = compile_mds_info(json_py,tables_json,mds_csv)
    
    ##############################Copy created tables into a final dictionary##################################
    emr = {}
    for i in tables_json.keys():
        for j in range(0,len(tables_json[i])):
            if i == 'pat_info':
                emr[tables_json[i][j]] = patient_info_data[tables_json[i][j]]
            elif i == 'dischargeInfo':
                emr[tables_json[i][j]] = discharge_data[tables_json[i][j]]
            elif i == 'diagnosesInfo':
                emr[tables_json[i]] = diagnoses_data[tables_json[i]]
            elif i == 'payorInfo':
                emr[tables_json[i]] = payors_data[tables_json[i]]
            elif i == 'facilityInfo':
                emr[tables_json[i][j]] = facility_data[tables_json[i][j]]
            elif i == 'documentsInfo':
                emr[tables_json[i]] = documents_data[tables_json[i]]
            elif i == 'oasisInfo':
                emr[tables_json[i]] = oasis_data[tables_json[i]]
            elif i == 'mdsInfo':
                emr[tables_json[i]] = mds_data[tables_json[i]]
                
    ######################################Perform Data Quality Checks for all the created tables#################################################
    dq_check_patInfo(json_py,emr)
    dq_checks_demographicId(json_py,emr)
    dq_check_contacts(json_py,emr)
    dq_check_discharge(json_py,emr)
    dq_check_dischargeIdentifiers(json_py,emr)
    dq_checks_diagnoses(json_py,emr)
    dq_checks_payors(json_py,emr)
    dq_checks_facility(json_py,emr)
    dq_checks_facilityId(json_py,emr)
    dq_checks_documents(json_py,emr)
    dq_checks_oasis(json_py,emr)
    dq_checks_mds(json_py,emr)
    ############################################################################################################################################
    
    
    ###########################################Remove complete duplicates from all the created tables################
    emr = remove_complete_duplicates(emr)
    ###############################################################################################################
    
    return emr
        
        
  
