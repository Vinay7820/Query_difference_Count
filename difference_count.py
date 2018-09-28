from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os import getenv
import pymssql
import config
import boto3
import smtplib
import time


def get_contacts(filename):
    """
    Return two lists - names, emails containing names and email addresses
    read from a file specified by filename.
    """

    names = []
    emails = []
    with open(filename, mode='r', encoding='utf-8') as contacts_file:
        for a_contact in contacts_file:
            names.append(a_contact.split()[0])
            emails.append(a_contact.split()[1])
    return names, emails

# This part is to query SQL Server and getting the response of the queries
server = getenv("PYMSSQL_SERVER") or config.PYMSSQL_SERVER
user = getenv("PYMSSQL_USERNAME") or config.PYMSSQL_USERNAME
password = getenv("PYMSSQL_PASSWORD") or config.PYMSSQL_PASSWORD

conn1 = pymssql.connect(server, user, password, "tempdb")
conn2 = pymssql.connect(server, user, password, "tempdb")
conn3 = pymssql.connect(server, user, password, "tempdb")
conn4 = pymssql.connect(server, user, password, "tempdb")
conn5 = pymssql.connect(server, user, password, "tempdb")

cursor1 = conn1.cursor()
cursor2 = conn2.cursor()
cursor3 = conn3.cursor()
cursor4 = conn4.cursor()
cursor5 = conn5.cursor()

# Connecting using Windows Authentication
'''
conn = pymssql.connect(
    host=r'dbhostname\myinstance',
    user=r'companydomain\username',
    password=PASSWORD,
    database='DatabaseOfInterest'
)
'''

cursor1.execute('SELECT count(*) FROM [EDW].tblLicensing_User_Data')
licensing_user_data_EDW = cursor1.fetchall()

cursor2.execute('SELECT COUNT(*) FROM [EDW].tbllicensing_settings_data')
licensing_settings_data_EDW = cursor2.fetchall()

cursor3.execute('SELECT count(*) FROM [EDW].tblLicensing_product_master')
licensing_product_master_EDW = cursor3.fetchall()

cursor4.execute('SELECT count(*) FROM [EDW].[tblprofileapi_mlsrelationship_daily_snapshot]')
tblprofileapi_mlsrelationship_daily_snapshot_EDW = cursor4.fetchall()

cursor5.execute('SELECT COUNT(*) FROM [EDW].[tblprofileapi_raw_daily_snapshot]')
tblprofileapi_raw_daily_snapshot_EDW = cursor5.fetchall()

conn1.close()
conn2.close()
conn3.close()
conn4.close()
conn5.close()

edw_query_response = [int(licensing_user_data_EDW), int(licensing_settings_data_EDW), int(licensing_product_master_EDW),
                      int(tblprofileapi_mlsrelationship_daily_snapshot_EDW), int(tblprofileapi_raw_daily_snapshot_EDW)]

# This part is to send email with the differences in the Athena and EDW SQL queries counts
query1 = "SELECT count(*) FROM biz_data.licensing_user_data ;"
query2 = "SELECT COUNT(*) FROM biz_data.licensing_settings_data ;"
query3 = "SELECT count(*) FROM biz_data.licensing_product_master ;"
query4 = "with delete_init as(select fulfillment_id, max(last_update_date_mst) as meta__last_update_date_mst from((select  fulfillment_id, meta__last_update_date_mst as last_update_date_mst  from cust_papi_pdt.profile_delete where fulfillment_id is not null)union(select  fulfillment_id,  cast(last_update_date_mst as timestamp) as last_update_date_mst from cust_papi_pdt.profile_hard_delete where fulfillment_id is not null) union(select  fulfillment_id,  cast(last_update_date_mst as timestamp) as last_update_date_mst from cust_papi_pdt.profile_soft_delete where fulfillment_id is not null)) a group by fulfillment_id), delete_final as (select a.fulfillment_id from(select  fulfillment_id, meta__last_update_date_mst from delete_init where fulfillment_id is not null) a join (select fulfillment_id, last_update_date_mst from biz_data.customer_profile_detail_snapshot_daily where data_snapshot_date_mst = (select max(data_snapshot_date_mst) from biz_data.customer_profile_detail_snapshot_daily) ) b on a.fulfillment_id = b.fulfillment_id and a.meta__last_update_date_mst > b.last_update_date_mst) select count(*) from biz_data.customer_profile_relationship_snapshot_daily WHERE data_snapshot_date_mst = (select max(data_snapshot_date_mst) from biz_data.customer_profile_relationship_snapshot_daily) and fulfillment_id not in (select distinct fulfillment_id from delete_final)"
query5 = "with delete_init as(select fulfillment_id, max(last_update_date_mst) as meta__last_update_date_mst from ((select  fulfillment_id, meta__last_update_date_mst as last_update_date_mst  from cust_papi_pdt.profile_delete where fulfillment_id is not null) union (select  fulfillment_id,  cast(last_update_date_mst as timestamp) as last_update_date_mst from cust_papi_pdt.profile_hard_delete where fulfillment_id is not null) union (select  fulfillment_id,  cast(last_update_date_mst as timestamp) as last_update_date_mst from cust_papi_pdt.profile_soft_delete where fulfillment_id is not null)) a group by fulfillment_id),delete_final as (select a.fulfillment_id from(select  fulfillment_id, meta__last_update_date_mst from delete_init where fulfillment_id is not null) a join (select fulfillment_id, last_update_date_mst from biz_data.customer_profile_detail_snapshot_daily where data_snapshot_date_mst = (select max(data_snapshot_date_mst) from biz_data.customer_profile_detail_snapshot_daily)) b on a.fulfillment_id = b.fulfillment_id and a.meta__last_update_date_mst > b.last_update_date_mst) SELECT count(*) FROM biz_data.customer_profile_detail_snapshot_daily p WHERE data_snapshot_date_mst = (select max(data_snapshot_date_mst) from biz_data.customer_profile_detail_snapshot_daily) and fulfillment_id not in (select distinct fulfillment_id from delete_final)"
athena_queries = [
    query1,
    query2,
    query3,
    query4,
    query5
]

# Athena connection initialization
athena = boto3.client('athena')

athena_query_response = []

# Executing query strings for each athena_queries list
for athena_query in athena_queries:
    response = athena.start_query_execution(
        QueryString=athena_query,
        QueryExecutionContext={
            'Database': '<THE_NAME_OF_THE_DATABASE>'
        },
        ResultConfiguration={
            'OutputLocation': '<S3-LOCATION>',
        }
    )

    time.sleep(25)
    response_for_get_query = athena.get_query_results(  ######## Query Result Information ###########
        QueryExecutionId=response['QueryExecutionId'],  ##Output of start_query_execution
        MaxResults=10
    )

    athena_query_response.append(int(response_for_get_query['Rows']['Data'][0]['VarCharValue']))

query_difference_edw_athena = [edw_query_response[e] - athena_query_response[e] for e in range(5)]

# Email sending for any difference between Athena and EDW queries
host = "smtp.gmail.com"
port = 587
username = '<SENDER-USER-NAME>'
password = '<SENDER-PASSWORD>'
from_email = username
to_list = ["<MULTI-SENDER-LIST>"]
names, emails = get_contacts('mycontacts.txt')

try:
    email_conn = smtplib.SMTP(host, port)
    email_conn.ehlo()
    email_conn.starttls()
    email_conn.login(username, password)
    for name, email in zip(names, emails):
        the_msg = MIMEMultipart("alternative")
        the_msg['Subject'] = "Athena and EDW Query Count Difference"
        the_msg['From'] = from_email
        # the_msg['To'] = to_list
        plain_txt = "Testing the message"
        html_txt = """\
        <html>
            <head></head>
            <body>
                <table style="border-collapse: collapse;">
                    <tr>
                        <th style="border: 1px solid black; padding: 10px; text-align: left;">Job Name</th>
                        <th style="border: 1px solid black; padding: 10px; text-align: left;">Athena Count</th>
                        <th style="border: 1px solid black; padding: 10px; text-align: left;">EDW Count</th>
                        <th style="border: 1px solid black; padding: 10px; text-align: left;">Difference</th>
                    </tr>
                    <tr>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">Licensing_user_data</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">Licensing_settings_data</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">Licensing_product_master</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">tblprofileapi_mlsrelationship_daily_snapshot</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">tblprofileapi_raw_daily_snapshot</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                        <td style="border: 1px solid black; padding: 10px; text-align: left;">{}</td>
                    </tr>
                </table>
            </body>
        </html>
        """.format(athena_query_response[0], edw_query_response[0], query_difference_edw_athena[0],
                   athena_query_response[1], edw_query_response[1], query_difference_edw_athena[1],
                   athena_query_response[2], edw_query_response[2], query_difference_edw_athena[2],
                   athena_query_response[3], edw_query_response[3], query_difference_edw_athena[3],
                   athena_query_response[4], edw_query_response[4], query_difference_edw_athena[4])
        part_1 = MIMEText(plain_txt, 'plain')
        part_2 = MIMEText(html_txt, 'html')
        the_msg.attach(part_1)
        the_msg.attach(part_2)
        email_conn.sendmail(from_email, to_list, the_msg.as_string())
        email_conn.quit()
except smtplib.SMTPException:
    print("error sending message")