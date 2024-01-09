from databricks import sql
import pandas as pd
from datetime import datetime 
import warnings
warnings.filterwarnings("ignore")

def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0914-072437-cwq0vku"#_QA_cluster
    access_token="dapicb69299bf00f93c358540bfbf746845c"#_QA_access_token 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection
# databricks()

def query_exe():
    print("started...")
    conxn=databricks()
    cust=['Bolt Agency','KraftLake','Progressive','bolt access','Liberty Mutual']#,'USAA','Boltag','National General','Unify','Elephant','Better Agency','Liberty Mutual X']
    for cust in cust:
        qry=f""" select ED.ENTITY_ID,ED.ENTITY_NUM,ED.ENTITY_FULL_NM,ED.ENTITY_ORGANIZATION_NM,EC.ENTITY_EMAIL,EC.ENTITY_MOBILE_NUM,PAD.APPLICATION_ID,PAD.APPLICATION_NUM,PAD.PLATFORM_APPLICATION_NUM,PADB.EXTERNAL_LEAD_NUM,padr.AGENT_ENTITY_ID,padr.AGENT_ENTITY_NUM,eedr.ENTITY_EXTERNAL_NUM,eedr.ENTITY_FULL_NM,eecr.ENTITY_EMAIL,pado.AGENT_ENTITY_ID,pado.AGENT_ENTITY_NUM,eedo.ENTITY_EXTERNAL_NUM,eedo.ENTITY_FULL_NM,eeco.ENTITY_EMAIL,chdo.HIERARCHY_NUM,chdo.HIERARCHY_NM,chdc.HIERARCHY_NUM,chdc.HIERARCHY_NM,
COALESCE(HOM.ENTITY_STATE_DESC, MAIL.ENTITY_STATE_DESC , BILL.ENTITY_STATE_DESC, WORK.ENTITY_STATE_DESC,PROP.APPLICATION_STATE_DESC,MSB.APPLICATION_STATE_DESC,MAIL2.APPLICATION_STATE_DESC),
COALESCE(HOM.ENTITY_CITY,MAIL.ENTITY_CITY, BILL.ENTITY_CITY, WORK.ENTITY_CITY,PROP.APPLICATION_CITY,MSB.APPLICATION_CITY,MAIL2.APPLICATION_CITY),
COALESCE(HOM.ENTITY_ZIP, MAIL.ENTITY_ZIP, BILL.ENTITY_ZIP, WORK.ENTITY_ZIP,PROP.APPLICATION_ZIP,MSB.APPLICATION_ZIP,MAIL2.APPLICATION_ZIP),
COALESCE(CONCAT(HOM.ADDRESS_LINE_1,HOM.ADDRESS_LINE_2),CONCAT(MAIL.ADDRESS_LINE_1,MAIL.ADDRESS_LINE_2),CONCAT(BILL.ADDRESS_LINE_1,BILL.ADDRESS_LINE_2),CONCAT(BILL.ADDRESS_LINE_1,BILL.ADDRESS_LINE_2),CONCAT(PROP.APPLICATION_ADDRESS_LINE_1,PROP.APPLICATION_ADDRESS_LINE_2),CONCAT(MSB.APPLICATION_ADDRESS_LINE_1,MSB.APPLICATION_ADDRESS_LINE_2),CONCAT(MAIL2.APPLICATION_ADDRESS_LINE_1,MAIL2.APPLICATION_ADDRESS_LINE_2)),
PROP.APPLICATION_STATE_DESC,
PROP.APPLICATION_CITY,  
PROP.APPLICATION_ZIP,
CONCAT(PROP.APPLICATION_ADDRESS_LINE_1,PROP.APPLICATION_ADDRESS_LINE_2),
MAIL.ENTITY_STATE_DESC,
MAIL.ENTITY_CITY,
MAIL.ENTITY_ZIP,CONCAT(MAIL.ADDRESS_LINE_1,MAIL.ADDRESS_LINE_2),
ED.ENTITY_BUSINESS_CATEGORY_DESC,
PAD.APPLICATION_FIRST_PRODUCT_DESC,
MST1.CUSTOMER_NM,
MST2.MST_TABLE_SOURCE_VALUE_DESC,
ED.ENTITY_SOURCE_DESC, 
ED.ENTITY_CHANNEL_NM,
CAST(ED.ENTITY_CREATION_DT AS DATE),
CAST(ED.ENTITY_CREATION_DT AS DATE),
CAST(PAD.APPLICATION_CREATED_DT AS DATE),CASE WHEN ED.ACTIVE_FLAG='YES' THEN 1 ELSE 0 END AS ACTIVE_FLAG,
PAD.TEST_FLAG from adp_core.dwh.ENT_ENTITY_DETAILS ED 
INNER JOIN adp_core.dwh.POL_APPLICATION_DETAILS PAD ON ED.ENTITY_ID=PAD.APPLICATION_ENTITY_ID 
left JOIN adp_core.dwh.ENT_ENTITY_CONTACT EC ON ED.ENTITY_ID=EC.ENTITY_ID
JOIN adp_core.MST.MST_CUSTOMER_DETAILS MST1 ON ED.CUSTOMER_ID=MST1.CUSTOMER_ID
left JOIN adp_core.dwh.POL_APPLICATION_DETAILS_BOLT PADB ON PADB.APPLICATION_ID=PAD.APPLICATION_ID
left join (Select *, row_number() over (partition by application_id order by last_update_dt desc) as RN from adp_core.dwh.pol_application_agent_details where agent_assignment_type_desc='REPRESENTATIVE' and hierarchy_classification_cd='2') padr on pad.application_id=padr.application_id
and pad.customer_id=padr.customer_id and padr.RN=1
left join adp_core.dwh.ent_entity_details eedr on eedr.entity_id=padr.agent_entity_id and eedr.customer_id=padr.customer_id
left join adp_core.dwh.ent_entity_contact eecr on eedr.entity_id=eecr.entity_id and eedr.customer_id=eecr.customer_id
left join (Select *, row_number() over (partition by application_id order by last_update_dt desc) as RN from adp_core.dwh.pol_application_agent_details where agent_assignment_type_desc='ORIGINATOR' and hierarchy_classification_cd='1') pado on pad.application_id=pado.application_id
and pad.customer_id=pado.customer_id and pado.RN=1
left join adp_core.dwh.ent_entity_details eedo on eedo.entity_id=pado.agent_entity_id and eedo.customer_id=pado.customer_id
left join adp_core.dwh.ent_entity_contact eeco on eedo.entity_id=eeco.entity_id and eedo.customer_id=eeco.customer_id
left join (Select *, row_number() over (partition by application_id order by last_update_dt desc) as RN from adp_core.dwh.pol_application_agent_details where agent_assignment_type_desc='ORIGINATOR' and hierarchy_classification_desc='OWNER HIERARCHY') padoh on pad.application_id=padoh.application_id
and pad.customer_id=padoh.customer_id and padoh.RN=1
left join adp_core.admin.cfg_hierarchy_details chdo on chdo.hierarchy_num=padoh.hierarchy_num and chdo.customer_id=padoh.customer_id
left join (Select *, row_number() over (partition by application_id order by last_update_dt desc) as RN from adp_core.dwh.pol_application_agent_details where agent_assignment_type_desc='REPRESENTATIVE' and hierarchy_classification_desc='CREATOR HIERARCHY') padch on pad.application_id=padch.application_id
and pad.customer_id=padch.customer_id and padch.RN=1
left join adp_core.admin.cfg_hierarchy_details chdc on chdc.hierarchy_num=padch.hierarchy_num and chdc.customer_id=padch.customer_id
LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='HOME ADDRESS')HOM ON ED.ENTITY_ID=HOM.ENTITY_ID 
LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='MAILING ADDRESS')MAIL ON ED.ENTITY_ID=MAIL.ENTITY_ID 
LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='BILLING ADDRESS')BILL ON ED.ENTITY_ID=BILL.ENTITY_ID 
LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='WORK ADDRESS')WORK ON ED.ENTITY_ID=WORK.ENTITY_ID 
LEFT JOIN (SELECT * FROM adp_core.dwh.POL_APPLICATION_ADDRESS_DETAILS WHERE ADDRESS_TYPE_DESC='PROPERTY ADDRESS')PROP ON PROP.APPLICATION_ID=PAD.APPLICATION_ID 
LEFT JOIN (SELECT * FROM adp_core.dwh.POL_APPLICATION_ADDRESS_DETAILS WHERE ADDRESS_TYPE_DESC='MAILING ADDRESS')MAIL2 ON MAIL2.APPLICATION_ID=PAD.APPLICATION_ID 
LEFT JOIN (SELECT * FROM adp_core.dwh.POL_APPLICATION_ADDRESS_DETAILS WHERE ADDRESS_TYPE_DESC='MSB VALUATION ADDRESS')MSB ON MSB.APPLICATION_ID=PAD.APPLICATION_ID 
LEFT  JOIN adp_core.MST.MST_MASTER_VALUE_LIST MST2 ON ED.SUB_CUSTOMER_ID=MST2.MST_TABLE_SOURCE_VALUE_CD AND ED.CUSTOMER_ID=MST2.CUSTOMER_ID AND MST2.MST_TABLE_BUSINESS_NAME = 'MST_SUB_CUSTOMER'
WHERE MST1.CUSTOMER_NM ='{cust}' and ed.audit_sys_id='12' and pad.audit_sys_id='12'
except
select CONSUMER_ID,CONSUMER_NUM,CONSUMER_FULL_NM,CONSUMER_ORGANIZATION_NM,CONSUMER_EMAIL,CONSUMER_MOBILE_NUM,APPLICATION_ID,APPLICATION_NUM,PLATFORM_APPLICATION_NUM,EXTERNAL_LEAD_NUM,SERVICING_AGENT_ID,SERVICING_AGENT_NUM,SERVICING_AGENT_EXTERNAL_NUM,SERVICING_AGENT_NM,SERVICING_AGENT_EMAIL,WRITING_AGENT_ID,WRITING_AGENT_NUM,WRITING_AGENT_EXTERNAL_NUM,WRITING_AGENT_NM,WRITING_AGENT_EMAIL,OWNER_HIERARCHY_NUM,OWNER_HIERARCHY_NM,CREATOR_HIERARCHY_NUM,CREATOR_HIERARCHY_NM,REPORTING_STATE_DESC,REPORTING_CITY,REPORTING_ZIP,REPORTING_ADDRESS,PROPERTY_STATE_DESC,PROPERTY_CITY,PROPERTY_ZIP,PROPERTY_ADDRESS,MAILING_STATE_DESC,MAILING_CITY,MAILING_ZIP,MAILING_ADDRESS,PRODUCT_TYPE_DESC,PRODUCT_DESC,CUSTOMER_NM,SUB_CUSTOMER_NM,TRANSACTION_FLOW_DESC,TRANSACTION_CHANNEL_NM,TRANSACTION_DT,CONSUMER_CREATED_DT,APPLICATION_STARTED_DATE,COUNT_OF_CONSUMER_STARTED,TEST_FLAG 
from adp_core.dn.dn_sales_funnel where activity_type_desc = 'APPLICATION CREATED' and CUSTOMER_NM = '{cust}' and audit_sys_id='12' """
        df1=pd.read_sql(qry,conxn)#.to_string(index=False)
        print(df1,cust)
query_exe()

# def query_exe():
#     conxn=databricks()
#     # cust=[2,14,13,11,9,24,17,22,30,26,38,36]
#     # for cust in cust:
#     qry=f""" select count(*) cnt from adp_core.dl2.policy"""
#     df1=pd.read_sql(qry,conxn)
#     print(df1['cnt'][0])
# query_exe()