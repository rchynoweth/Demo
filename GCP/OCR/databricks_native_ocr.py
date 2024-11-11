# Databricks notebook source
# MAGIC %md
# MAGIC # Object Character Recognition on Databricks - IN PROGRESS
# MAGIC
# MAGIC We are going to use the Databricks Labs [project]() as an example. The base requirement is to install the following maven dependency on the cluster: `com.databricks.labs:tika-ocr:0.1.4`.  
# MAGIC
# MAGIC
# MAGIC
# MAGIC This notebook shows an end to end workflow that extracts text from images and uses an LLM to summarize the results so that they can be structured and analyzed. 
# MAGIC
# MAGIC This notebook uses the following image to text [model](https://huggingface.co/stepfun-ai/GOT-OCR2_0), Llama 3.1 __ language model, and runs on DBR ML 15LTS with GPUs. 
# MAGIC
# MAGIC https://github.com/philschmid/document-ai-transformers
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# set and get notebook parameters 
dbutils.widgets.text('catalog_name', '')
dbutils.widgets.text('schema_name', '')
dbutils.widgets.text('volume_name', '')

catalog_name = dbutils.widgets.get('catalog_name')
schema_name = dbutils.widgets.get('schema_name')
volume_name = dbutils.widgets.get('volume_name')

print(f"{catalog_name} | {schema_name} | {volume_name}")

# COMMAND ----------

# from transformers import AutoTokenizer, AutoModelForCausalLM, AutoModel
# import requests
# from PIL import Image, ImageEnhance
# from io import BytesIO
# import torch

from transformers import TrOCRProcessor, VisionEncoderDecoderModel, DonutProcessor
from PIL import Image
import torch

# COMMAND ----------

device = torch.device('cuda:0' if torch.cuda.is_available else 'cpu')
print(device)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Notes 
# MAGIC
# MAGIC - `microsoft/trocr-large-printed`: trained on receipts and should be used on single text line images. 
# MAGIC - `microsoft/trocr-large-str`: fine-tuned on the training sets of IC13, IC15, IIIT5K, SVT and used on single text line images. 
# MAGIC - `microsoft/trocr-large-handwritten`: fune tuned on the IAM dataset and used for single line of text. 
# MAGIC - There are "base" models as well which only do the initial training and are not fine-tuned to a specific dataset. This allows Microsoft to train a single base model then fine tune it to a task as needed. 
# MAGIC - All MSFT models expect single line of text to work best so we will need to batch our images. 

# COMMAND ----------

model_name = 'microsoft/trocr-large-handwritten' 
image_path = "/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg"  
image = Image.open(image_path).convert('RGB').rotate(-90, expand=True)

processor = TrOCRProcessor.from_pretrained(model_name)
model = VisionEncoderDecoderModel.from_pretrained(model_name).to(device)

# COMMAND ----------

image.size

# COMMAND ----------

# # cropped_image = image.crop((left, upper, right, lower))
# increments = 50
# i = 0
# while i <= image.size[0]:
#   img = image.crop((0, i, image.size[1], i+increments))
#   pixel_values = processor(img, return_tensors='pt').pixel_values.to(device)
#   generated_ids = model.generate(pixel_values, max_new_tokens=1000)
#   generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
#   print(generated_text)
#   i+=increments
# # img

# COMMAND ----------

# (3120, 4160)
# cropped_image = image.crop((left, upper, right, lower))

model.config.encoder.image_size = [4160, 400] # (height, width)
model.config.decoder.max_length = 512

img = image.crop((0, 1300, image.size[1], 1700))
# img = image.crop((0, i, image.size[1], i+increments))
pixel_values = processor(img, return_tensors='pt').pixel_values.to(device)
generated_ids = model.generate(pixel_values, max_new_tokens=1000)
generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
print(generated_text)

# COMMAND ----------

img

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

img.size

# COMMAND ----------

model = VisionEncoderDecoderModel.from_pretrained("naver-clova-ix/donut-base")

device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

# Load processor
processor = DonutProcessor.from_pretrained("naver-clova-ix/donut-base")

# set decoder start token id
# model.config.decoder_start_token_id = processor.tokenizer.convert_tokens_to_ids(['<s>'])[0]
# model.config.pad_token_id = processor.tokenizer.pad_token_id

# max_length = 512
# image_size = [4160, 3120]

# # update image_size of the encoder

# # model.config.encoder.image_size = processor.feature_extractor.size[::-1] # (height, width)
# model.config.encoder.image_size = image_size # (height, width)
# model.config.decoder.max_length = max_length

# # resize processor and model to match
# processor.feature_extractor.size = image_size[::-1] # should be (width, height)
# processor.feature_extractor.do_align_long_axis = False

# COMMAND ----------

pixel_values = processor(img, return_tensors='pt').pixel_values.to(device)
generated_ids = model.generate(pixel_values, max_new_tokens=1000)
generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
print(generated_text)

# COMMAND ----------

img

# COMMAND ----------


