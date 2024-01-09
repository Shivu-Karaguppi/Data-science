from databricks import sql
import pandas as pd
from datetime import datetime 
import warnings
warnings.filterwarnings("ignore")

def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0525-070435-t5v0j694"#my_cluster
    access_token="dapid0901d27ea66595762a8a5484812b458"#my_access_token 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection
# databricks()

def query_exe():
    conxn=databricks()
    cust=[2,13,11,9,24,17,22,30,26,38,36]
    for cust in cust:
        qry=f"""select A.ENTITY_ID,A.ENTITY_NUM,A.ENTITY_EXTERNAL_NUM,B.CUSTOMER_ID,B.CUSTOMER_NM,A.SUB_CUSTOMER_ID,mst.MST_TABLE_SOURCE_VALUE_DESC,mst.MST_TABLE_BUSINESS_VALUE_CD_1,A.ENTITY_FIRST_NM,A.ENTITY_MIDDLE_NM,A.ENTITY_LAST_NM,A.ENTITY_FULL_NM,A.ENTITY_DOB,A.ENTITY_CREATION_DT,A.ENTITY_EXPIRY_DT,A.ACTIVE_FLAG,A.ENTITY_SOURCE_CD,A.ENTITY_SOURCE_DESC,A.ENTITY_CHANNEL_NM,A.ENTITY_ORGANIZATION_NM,A.ENTITY_TYPE_CD,A.ENTITY_TYPE_DESC,A.ENTITY_STATUS_CD,A.ENTITY_STATUS_DESC,A.ENTITY_BUSINESS_CATEGORY_CD,A.ENTITY_BUSINESS_CATEGORY_DESC,A.HIERARCHY_ID,A.HIERARCHY_NUM,AL.AGENT_ENTITY_ID,AL.AGENT_ENTITY_NUM,AL2.AGENT_ENTITY_ID,AL2.AGENT_ENTITY_NUM,AL3.HIERARCHY_ID,AL3.HIERARCHY_NUM,EC.ENTITY_EMAIL,EC.ENTITY_ALTERNATE_EMAIL,EC.ENTITY_RES_NUM,EC.ENTITY_FAX_NUM,EC.ENTITY_WORK_PHONE_NUM,EC.ENTITY_MOBILE_NUM,EC.ENTITY_PAGER_NUM,
COALESCE(HOM.ADDRESS_LINE_1,MAIL.ADDRESS_LINE_1,BILL.ADDRESS_LINE_1,WORK.ADDRESS_LINE_1),
COALESCE(HOM.ADDRESS_LINE_2, MAIL.ADDRESS_LINE_2, BILL.ADDRESS_LINE_2, WORK.ADDRESS_LINE_2),
COALESCE(HOM.ENTITY_CITY,MAIL.ENTITY_CITY, BILL.ENTITY_CITY, WORK.ENTITY_CITY),
COALESCE(HOM.ENTITY_STATE_CD, MAIL.ENTITY_STATE_CD, BILL.ENTITY_STATE_CD, WORK.ENTITY_STATE_CD),
COALESCE(HOM.ENTITY_STATE_DESC, MAIL.ENTITY_STATE_DESC , BILL.ENTITY_STATE_DESC, WORK.ENTITY_STATE_DESC),
COALESCE(HOM.ENTITY_ZIP, MAIL.ENTITY_ZIP, BILL.ENTITY_ZIP, WORK.ENTITY_ZIP),
COALESCE(HOM.ENTITY_COUNTY, MAIL.ENTITY_COUNTY, BILL.ENTITY_COUNTY, WORK.ENTITY_COUNTY),
HOM.ADDRESS_LINE_1,HOM.ADDRESS_LINE_2,HOM.ENTITY_CITY,HOM.ENTITY_STATE_CD,HOM.ENTITY_STATE_DESC,HOM.ENTITY_ZIP,HOM.ENTITY_COUNTY,
MAIL.ADDRESS_LINE_1,MAIL.ADDRESS_LINE_2,MAIL.ENTITY_CITY,MAIL.ENTITY_STATE_CD,MAIL.ENTITY_STATE_DESC,MAIL.ENTITY_ZIP,MAIL.ENTITY_COUNTY,
BILL.ADDRESS_LINE_1,BILL.ADDRESS_LINE_2,BILL.ENTITY_CITY,BILL.ENTITY_STATE_CD,BILL.ENTITY_STATE_DESC,BILL.ENTITY_ZIP,BILL.ENTITY_COUNTY,
WORK.ADDRESS_LINE_1,WORK.ADDRESS_LINE_2,WORK.ENTITY_CITY,WORK.ENTITY_STATE_CD,WORK.ENTITY_STATE_DESC,WORK.ENTITY_ZIP,WORK.ENTITY_COUNTY,
A.AUDIT_SRC_ID,A.AUDIT_SYS_ID
 from  adp_core.dwh.ENT_ENTITY_DETAILS A
 JOIN adp_core.mst.MST_CUSTOMER_DETAILS B ON A.CUSTOMER_ID=B.CUSTOMER_ID
 left JOIN adp_core.mst.MST_MASTER_VALUE_LIST mst ON A.SUB_CUSTOMER_ID=mst.MST_TABLE_SOURCE_VALUE_CD AND A.CUSTOMER_ID=mst.CUSTOMER_ID AND mst.MST_TABLE_BUSINESS_NAME = 'MST_SUB_CUSTOMER'
 left JOIN adp_core.dwh.AGT_LEADS AL ON A.ENTITY_ID=AL.LEADS_ENTITY_ID AND A.CUSTOMER_ID=AL.CUSTOMER_ID AND AL.LEADS_ASSOCIATE_TYPE_DESC='REPRESENTATIVE' AND AL.HIERARCHY_CLASSIFICATION_DESC='CREATOR HIERARCHY'
 left JOIN adp_core.dwh.AGT_LEADS AL2 ON A.ENTITY_ID=AL2.LEADS_ENTITY_ID AND A.CUSTOMER_ID=AL2.CUSTOMER_ID AND AL2.LEADS_ASSOCIATE_TYPE_DESC='ORIGINATOR' AND AL2.HIERARCHY_CLASSIFICATION_DESC='OWNER HIERARCHY'
 left JOIN adp_core.dwh.AGT_LEADS AL3 ON A.ENTITY_ID=AL3.LEADS_ENTITY_ID AND A.CUSTOMER_ID=AL3.CUSTOMER_ID AND AL3.HIERARCHY_CLASSIFICATION_DESC='OWNER HIERARCHY' and AL3.LEADS_ASSOCIATE_TYPE_DESC='ORIGINATOR' 
 LEFT JOIN adp_core.dwh.ENT_ENTITY_CONTACT EC  ON EC.ENTITY_ID=A.ENTITY_ID AND EC.CUSTOMER_ID=A.CUSTOMER_ID
 LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='HOME ADDRESS')HOM ON A.ENTITY_ID=HOM.ENTITY_ID 
 LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='MAILING ADDRESS')MAIL ON A.ENTITY_ID=MAIL.ENTITY_ID 
 LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='BILLING ADDRESS')BILL ON A.ENTITY_ID=BILL.ENTITY_ID 
 LEFT JOIN (SELECT * FROM adp_core.dwh.ENT_ENTITY_ADDRESS WHERE ADDRESS_TYPE_DESC='WORK ADDRESS')WORK ON A.ENTITY_ID=WORK.ENTITY_ID
 WHERE A.ENTITY_CATEGORY_DESC = 'CONSUMER' and A.customer_id={cust}
 except 
 select CONSUMER_ID,CONSUMER_NUM,CONSUMER_EXTERNAL_NUM,CUSTOMER_ID,CUSTOMER_NM,SUB_CUSTOMER_ID,SUB_CUSTOMER_NM,SUB_CUSTOMER_EXTERNAL_NUM,CONSUMER_FIRST_NM,CONSUMER_MIDDLE_NM,CONSUMER_LAST_NM,CONSUMER_FULL_NM,CONSUMER_DOB,CONSUMER_CREATION_DT,CONSUMER_EXPIRY_DT,ACTIVE_FLAG,CONSUMER_FLOW_CD,CONSUMER_FLOW_DESC,CONSUMER_CHANNEL_NM,CONSUMER_ORGANIZATION_NM,CONSUMER_TYPE_CD,CONSUMER_TYPE_DESC,CONSUMER_STATUS_CD,CONSUMER_STATUS_DESC,CONSUMER_BUSINESS_CATEGORY_CD,CONSUMER_BUSINESS_CATEGORY_DESC,HIERARCHY_ID,HIERARCHY_NUM,SERVICING_AGENT_ENTITY_ID,SERVICING_AGENT_ENTITY_NUM,WRITING_AGENT_ENTITY_ID,WRITING_AGENT_ENTITY_NUM,OWNER_HIERARCHY_ID,OWNER_HIERARCHY_NUM,CONSUMER_EMAIL,CONSUMER_ALTERNATE_EMAIL,CONSUMER_RES_NUM,CONSUMER_FAX_NUM,CONSUMER_WORK_PHONE_NUM,CONSUMER_MOBILE_NUM,CONSUMER_PAGER_NUM,CONSUMER_REPORTING_ADDRESS_LINE_1,CONSUMER_REPORTING_ADDRESS_LINE_2,CONSUMER_REPORTING_CITY,CONSUMER_REPORTING_STATE_CD,CONSUMER_REPORTING_STATE_DESC,CONSUMER_REPORTING_ZIP,CONSUMER_REPORTING_COUNTY,HOME_ADDRESS_LINE_1,HOME_ADDRESS_LINE_2,CONSUMER_HOME_CITY,CONSUMER_HOME_STATE_CD,CONSUMER_HOME_STATE_DESC,CONSUMER_HOME_ZIP,CONSUMER_HOME_COUNTY,MAILING_ADDRESS_LINE_1,MAILING_ADDRESS_LINE_2,CONSUMER_MAILING_CITY,CONSUMER_MAILING_STATE_CD,CONSUMER_MAILING_STATE_DESC,CONSUMER_MAILING_ZIP,CONSUMER_MAILING_COUNTY,BILLING_ADDRESS_LINE_1,BILLING_ADDRESS_LINE_2,CONSUMER_BILLING_CITY,CONSUMER_BILLING_STATE_CD,CONSUMER_BILLING_STATE_DESC,CONSUMER_BILLING_ZIP,CONSUMER_BILLING_COUNTY,WORK_ADDRESS_LINE_1,WORK_ADDRESS_LINE_2,CONSUMER_WORK_CITY,CONSUMER_WORK_STATE_CD,CONSUMER_WORK_STATE_DESC,CONSUMER_WORK_ZIP,CONSUMER_WORK_COUNTY,AUDIT_SRC_ID,AUDIT_SYS_ID--,AUDIT_BATCH_ID,AUDIT_SUB_BATCH_ID,AUDIT_IU_FLAG,LAST_UPDATE_DT,LAST_UPDATE_USER_ID,AUDIT_TABLE_ID,AUDIT_STORAGE_LEVEL
 from adp_core.dn.dn_consumer WHERE customer_id={cust}  """
        df1=pd.read_sql(qry,conxn)
        print(df1,cust)
        # print(qry,"\n \n")
query_exe()
