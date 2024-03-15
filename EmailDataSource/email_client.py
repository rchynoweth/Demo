import email
from email.header import decode_header
import pandas as pd
from pyspark.sql.functions import to_timestamp, lit



class EmailClient():

  def __init__(self, spark, attachement_folder):
    self.spark = spark 
    self.attachement_folder = attachement_folder

    self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  
  def read_email_file(self, file_path):
    return email.message_from_file(open(file_path))
  

  def read_email_from_bytes(self, email_bytes):
    return email.message_from_bytes(email_bytes)
  

  def load_email_as_spark_df(self, msg):
    data_keys = msg.keys()
    df_columns = ['MIME-Version', 'Date', 'References', 'In-Reply-To', 'Bcc', 'Message-ID', 'Subject', 'From', 'To', 'Cc', 'Content-Type']
    email_data = {}
    for k in data_keys:
      email_data[k] = decode_header(msg.get(k))[0]

    pdf = pd.DataFrame(email_data).dropna()

    _, save_path, _, _ ,_ = self.get_email_attachement(msg)

    df = (self.spark.createDataFrame(pdf)
               .withColumn("Date", to_timestamp("Date", "EEE, dd MMM yyyy HH:mm:ss Z"))
               .withColumn("email_body", lit(self.get_email_body(msg)))
               .withColumn("attachement_location", lit(save_path))
              )
    
    return df 

  def get_email_body(self, msg):
    # this only gets the most recent email
    # we don't need the entire thread 
    if msg.get_content_subtype() == 'mixed':
      return msg.get_payload()[0].get_payload()[0].get_payload()
    elif msg.get_content_subtype() == 'alternative':
      return msg.get_payload()[0].get_payload() 
    else :
      return None

  def get_email_attachement(self, msg):
    attachment = msg.get_payload()[1]
    file_name = attachment.get_filename()
    attachment_id = attachment.get('Content-ID').strip("<").strip(">")
    save_path = f"{self.attachement_folder}/{attachment_id}_{file_name}"
    attachment_contents = attachment.get_payload(decode=True)

    if attachment_id is not None:
      return (attachment_contents, save_path, file_name, attachment_id, attachment)
    else :
      return (None, None, None, None, None)
    

  def save_attachment(self, attachment_contents, save_path):
    return open(save_path, 'wb').write(attachment_contents)