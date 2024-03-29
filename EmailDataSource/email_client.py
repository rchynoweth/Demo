import email
import imaplib
from email.header import decode_header
import pandas as pd
from pyspark.sql.functions import to_timestamp, lit



class EmailClient():
  """
  Class to handle reading and extracting data from email messages
  """

  def __init__(self, spark, attachment_folder):
    self.spark = spark 
    self.attachment_folder = attachment_folder

    self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  def gmail_client(self, username, password): 
    """ Creates connection to gmail """
    imap_url = 'imap.gmail.com'
    # this is done to make SSL connection with GMAIL
    conn = imaplib.IMAP4_SSL(imap_url) 
    # logging the user in
    conn.login(username, password) 
    return conn
  
  def search_gmail(self, conn, subject, from_acct, mailbox='inbox'):
    conn.select(mailbox)
    search_criteria = f'(FROM "{from_acct}" SUBJECT "{subject}")' # can be a param instead
    # get email ids that match criteria 
    result, data = conn.search(None, search_criteria )
    # get the latest id for purposes of the demo
    ids = data[0] # data is a list.
    id_list = ids.split() # ids is a space separated string
    latest_email_id = id_list[-1] # get the latest
    result, data = conn.fetch(latest_email_id, "(RFC822)")
    return data[0][1] 

  
  def read_email_file(self, file_path):
    """
    Read email message from file 
    """
    return email.message_from_file(open(file_path))
  

  def read_email_from_bytes(self, email_bytes):
    """
    Read email message from bytes 
    """
    return email.message_from_bytes(email_bytes)
  

  def load_email_as_spark_df(self, msg):
    """
    Load email data into Spark Dataframe
    """
    data_keys = msg.keys()
    df_columns = ['MIME-Version', 'Date', 'References', 'In-Reply-To', 'Bcc', 'Message-ID', 'Subject', 'From', 'To', 'Cc', 'Content-Type']
    email_data = {}
    for k in data_keys:
      email_data[k] = decode_header(msg.get(k))[0]

    pdf = pd.DataFrame(email_data).dropna()

    _, save_path, _, _ ,_ = self.get_email_attachment(msg)

    df = (self.spark.createDataFrame(pdf)
               .withColumn("Date", to_timestamp("Date", "EEE, dd MMM yyyy HH:mm:ss Z"))
               .withColumn("email_body", lit(self.get_email_body(msg)))
               .withColumn("attachment_location", lit(save_path))
              )
    
    return df 

  def get_email_body(self, msg):
    """
    Get the email body
    """
    # this only gets the most recent email
    # we don't need the entire thread 
    if msg.get_content_subtype() == 'mixed':
      return msg.get_payload()[0].get_payload()[0].get_payload()
    elif msg.get_content_subtype() == 'alternative':
      return msg.get_payload()[0].get_payload() 
    else :
      return None

  def get_email_attachment(self, msg):
    """
    Get metadata about possible attachments
    """
    attachment = msg.get_payload()[1]
    file_name = attachment.get_filename()
    attachment_id = attachment.get('Content-ID').strip("<").strip(">") if attachment.get('Content-ID') is not None else None
    save_path = f"{self.attachment_folder}/{attachment_id}_{file_name}"
    attachment_contents = attachment.get_payload(decode=True)

    if attachment_id is not None:
      return (attachment_contents, save_path, file_name, attachment_id, attachment)
    else :
      return (None, None, None, None, None)
    

  def save_attachment(self, attachment_contents, save_path):
    """
    Save the attachment to a file
    """
    return open(save_path, 'wb').write(attachment_contents)
  

  def save_email_data_as_table(self, df, table_name):
    """
    Save email dataframe to table
    """
    df.write.mode('append').saveAsTable(table_name)